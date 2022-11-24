// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventlog

import (
	// standard libraries.
	"context"
	stderr "errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	el "github.com/linkall-labs/vanus/client/internal/vanus/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/record"
	vlog "github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultRetryTimes = 10
	pollingThreshold  = 200 // in milliseconds.
	pollingPostSpan   = 100 // in milliseconds.
)

func NewEventLog(cfg *el.Config) Eventlog {
	log := &eventlog{
		cfg:         cfg,
		nameService: el.NewNameService(cfg.Endpoints),
		tracer: tracing.NewTracer("pkg.eventlog.impl",
			trace.SpanKindClient),
	}

	log.writableWatcher = WatchWritableSegment(log)
	log.readableWatcher = WatchReadableSegments(log)

	go func() {
		ch := log.writableWatcher.Chan()
		for {
			r, ok := <-ch
			if !ok {
				vlog.Debug(context.Background(), "eventlog quits writable watcher", map[string]interface{}{
					"eventlog": log.cfg.ID,
				})
				break
			}

			ctx, span := log.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if r != nil {
				log.updateWritableSegment(ctx, r)
			}

			log.writableWatcher.Wakeup()
			span.End()
		}
	}()
	log.writableWatcher.Start()

	go func() {
		ch := log.readableWatcher.Chan()
		for {
			rs, ok := <-ch
			if !ok {
				vlog.Debug(context.Background(), "eventlog quits readable watcher", map[string]interface{}{
					"eventlog": log.cfg.ID,
				})
				break
			}
			ctx, span := log.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if rs != nil {
				log.updateReadableSegments(ctx, rs)
			}

			log.readableWatcher.Wakeup()
			span.End()
		}
	}()
	log.readableWatcher.Start()

	return log
}

type eventlog struct {
	cfg         *el.Config
	nameService *el.NameService

	writableWatcher *WritableSegmentWatcher
	writableSegment *segment
	writableMu      sync.RWMutex

	readableWatcher  *ReadableSegmentsWatcher
	readableSegments []*segment
	readableMu       sync.RWMutex
	tracer           *tracing.Tracer
}

// make sure eventlog implements eventlog.EventLog.
var _ Eventlog = (*eventlog)(nil)

func (l *eventlog) ID() uint64 {
	return l.cfg.ID
}

func (l *eventlog) Close(ctx context.Context) {
	l.writableWatcher.Close()
	l.readableWatcher.Close()

	if l.writableSegment != nil {
		l.writableSegment.Close(ctx)
	}
	for _, segment := range l.readableSegments {
		segment.Close(ctx)
	}
}

func (l *eventlog) Writer() LogWriter {
	w := &logWriter{
		elog: l,
	}
	return w
}

func (l *eventlog) Reader(cfg ReaderConfig) LogReader {
	r := &logReader{
		elog: l,
		pos:  0,
		cfg:  cfg,
	}
	return r
}

func (l *eventlog) EarliestOffset(ctx context.Context) (int64, error) {
	rs, err := l.nameService.LookupReadableSegments(ctx, l.cfg.ID)
	if err != nil {
		return 0, err
	}
	if len(rs) == 0 {
		return 0, errors.ErrNotReadable
	}
	return rs[0].StartOffset, nil
}

func (l *eventlog) LatestOffset(ctx context.Context) (int64, error) {
	rs, err := l.nameService.LookupReadableSegments(ctx, l.cfg.ID)
	if err != nil {
		return 0, err
	}
	if len(rs) == 0 {
		return 0, errors.ErrNotReadable
	}
	return rs[len(rs)-1].EndOffset, nil
}

func (l *eventlog) Length(ctx context.Context) (int64, error) {
	// TODO(kai.jiangkai)
	return 0, nil
}

func (l *eventlog) QueryOffsetByTime(ctx context.Context, timestamp int64) (int64, error) {
	t := time.UnixMilli(timestamp)
	// get all segments
	var target *segment
	segs := l.fetchReadableSegments(ctx)

	if len(segs) == 0 || segs[0].firstEventBornAt.After(t) {
		return -1, errors.ErrNotFound
	}

	if segs[len(segs)-1].lastEventBornAt.Before(t) {
		// the target offset maybe in newer segment, refresh immediately
		l.refreshReadableSegments(ctx)
		segs = l.fetchReadableSegments(ctx)
	}

	for idx := range l.readableSegments {
		s := l.readableSegments[idx]
		if s.firstEventBornAt.Equal(t) {
			return s.startOffset, nil
		} else if s.lastEventBornAt.Equal(t) {
			return s.endOffset.Load(), nil
		} else if s.firstEventBornAt.Before(t) && s.lastEventBornAt.After(t) {
			target = s
			break
		}
	}

	if target == nil {
		// not found
		return -1, errors.ErrNotFound
	}

	return target.prefer.LookupOffset(ctx, t)
}

func (l *eventlog) updateWritableSegment(ctx context.Context, r *record.Segment) {
	if l.writableSegment != nil {
		if l.writableSegment.ID() == r.ID {
			_ = l.writableSegment.Update(ctx, r, true)
			return
		}
	}

	segment, err := newSegment(ctx, r, true)
	if err != nil {
		vlog.Error(context.Background(), "new segment failed", map[string]interface{}{
			vlog.KeyError: err,
		})
		return
	}

	l.writableMu.Lock()
	defer l.writableMu.Unlock()

	l.writableSegment = segment
}

func (l *eventlog) selectWritableSegment(ctx context.Context) (*segment, error) {
	segment := l.fetchWritableSegment(ctx)
	if segment == nil {
		return nil, errors.ErrNotWritable
	}
	return segment, nil
}

func (l *eventlog) fetchWritableSegment(ctx context.Context) *segment {
	l.writableMu.RLock()
	defer l.writableMu.RUnlock()

	if l.writableSegment == nil || !l.writableSegment.Writable() {
		// refresh
		func() {
			l.writableMu.RUnlock()
			defer l.writableMu.RLock()
			l.refreshWritableSegment(ctx)
		}()
	}

	return l.writableSegment
}

func (l *eventlog) refreshWritableSegment(ctx context.Context) {
	_ = l.writableWatcher.Refresh(ctx)
}

func (l *eventlog) updateReadableSegments(ctx context.Context, rs []*record.Segment) {
	segments := make([]*segment, 0, len(rs))
	for _, r := range rs {
		// TODO: find
		segment := func() *segment {
			for _, s := range l.readableSegments {
				if s.ID() == r.ID {
					return s
				}
			}
			return nil
		}()
		var err error
		if segment == nil {
			segment, err = newSegment(ctx, r, false)
		} else {
			err = segment.Update(ctx, r, false)
		}
		if err != nil {
			// FIXME: create or update segment failed
			continue
		}
		segments = append(segments, segment)
	}

	l.writableMu.Lock()
	defer l.writableMu.Unlock()

	l.readableSegments = segments
}

func (l *eventlog) selectReadableSegment(ctx context.Context, offset int64) (*segment, error) {
	segments := l.fetchReadableSegments(ctx)
	if len(segments) == 0 {
		return nil, errors.ErrNotReadable
	}
	// TODO: make sure the segments are in order.
	n := sort.Search(len(segments), func(i int) bool {
		return segments[i].EndOffset() > offset
	})
	if n < len(segments) {
		return segments[n], nil
	}
	if offset < segments[0].StartOffset() {
		return nil, errors.ErrUnderflow
	}
	if offset == segments[len(segments)-1].EndOffset() {
		return nil, errors.ErrOnEnd
	}
	return nil, errors.ErrOverflow
}

func (l *eventlog) fetchReadableSegments(ctx context.Context) []*segment {
	l.readableMu.RLock()
	defer l.readableMu.RUnlock()

	if len(l.readableSegments) == 0 {
		l.readableMu.RUnlock()
		defer l.readableMu.RLock()
		// refresh
		l.refreshReadableSegments(ctx)
	}

	return l.readableSegments
}

func (l *eventlog) refreshReadableSegments(ctx context.Context) {
	_ = l.readableWatcher.Refresh(ctx)
}

// logWriter is the writer of eventlog.
//
// Append is thread-safety.
type logWriter struct {
	elog *eventlog
	cur  *segment
	mu   sync.RWMutex
}

func (w *logWriter) Log() Eventlog {
	return w.elog
}

func (w *logWriter) Close(ctx context.Context) {
	// TODO: by jiangkai, 2022.10.19
}

func (w *logWriter) Append(ctx context.Context, event *ce.Event) (int64, error) {
	// TODO: async for throughput

	retryTimes := defaultRetryTimes
	for i := 1; i <= retryTimes; i++ {
		offset, err := w.doAppend(ctx, event)
		if err == nil {
			return offset, nil
		}

		switch err {
		case errors.ErrNotWritable, errors.ErrNotEnoughSpace, errors.ErrNoSpace:
			// full
			if i < retryTimes {
				continue
			}
		}

		return -1, err
	}

	return -1, errors.ErrUnknown
}

func (w *logWriter) doAppend(ctx context.Context, event *ce.Event) (int64, error) {
	segment, err := w.selectWritableSegment(ctx)
	if err != nil {
		return -1, err
	}
	offset, err := segment.Append(ctx, event)
	if err != nil {
		switch err {
		case errors.ErrNotWritable, errors.ErrNotEnoughSpace, errors.ErrNoSpace:
			segment.SetNotWritable()
		}
		return -1, err
	}
	return offset, nil
}

func (w *logWriter) selectWritableSegment(ctx context.Context) (*segment, error) {
	segment := func() *segment {
		w.mu.RLock()
		defer w.mu.RUnlock()
		if w.cur != nil && w.cur.Writable() {
			return w.cur
		}
		return nil
	}()

	if segment == nil {
		w.mu.Lock()
		defer w.mu.Unlock()

		segment = w.cur
		if segment == nil || !segment.Writable() { // double check
			var err error
			segment, err = w.elog.selectWritableSegment(ctx)
			if err != nil {
				return nil, err
			}
			w.cur = segment
		}
	}

	return segment, nil
}

type logReader struct {
	elog *eventlog
	pos  int64
	cur  *segment
	cfg  ReaderConfig
}

func (r *logReader) Log() Eventlog {
	return r.elog
}

func (r *logReader) Close(ctx context.Context) {
	// TODO: by jiangkai, 2022.10.19
}

func (r *logReader) Read(ctx context.Context, size int16) ([]*ce.Event, error) {
	if r.cur == nil {
		segment, err := r.elog.selectReadableSegment(ctx, r.pos)
		if stderr.Is(err, errors.ErrOnEnd) {
			r.elog.refreshReadableSegments(ctx)
			segment, err = r.elog.selectReadableSegment(ctx, r.pos)
		}
		if err != nil {
			return nil, err
		}
		r.cur = segment
	}

	events, err := r.cur.Read(ctx, r.pos, size, uint32(r.pollingTimeout(ctx)))
	if err != nil {
		if stderr.Is(err, errors.ErrOverflow) {
			r.elog.refreshReadableSegments(ctx)
			if r.switchSegment(ctx) {
				return nil, errors.ErrTryAgain
			}
		}
		return nil, err
	}

	r.pos += int64(len(events))
	if r.pos == r.cur.EndOffset() {
		r.switchSegment(ctx)
	}

	return events, nil
}

func (r *logReader) pollingTimeout(ctx context.Context) int64 {
	if r.cfg.PollingTimeout == 0 {
		return 0
	}
	if dl, ok := ctx.Deadline(); ok {
		switch timeout := time.Until(dl).Milliseconds() - pollingPostSpan; {
		case timeout < pollingThreshold:
			return 0
		case timeout < r.cfg.PollingTimeout:
			return timeout
		}
	}
	return r.cfg.PollingTimeout
}

func (r *logReader) switchSegment(ctx context.Context) bool {
	// switch to next segment
	segment, err := r.elog.selectReadableSegment(ctx, r.pos)
	if err != nil {
		r.cur = nil
		return false
	}
	r.cur = segment
	return true
}

func (r *logReader) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	// TODO
	if whence == io.SeekStart {
		r.pos = offset
		r.cur = nil
		return offset, nil
	}
	return -1, errors.ErrInvalidArgument
}
