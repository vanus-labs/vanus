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
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"io"
	"sort"
	"sync"
	"time"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	veld "github.com/linkall-labs/vanus/client/internal/vanus/discovery/eventlog"
	vdr "github.com/linkall-labs/vanus/client/internal/vanus/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

const (
	defaultRetryTimes = 3
	pollingThreshold  = 200 // in milliseconds.
	pollingPostSpan   = 100 // in milliseconds.
)

func UseDistributedLog(scheme string) {
	eventlog.Register(scheme, func(cfg *eventlog.Config) (eventlog.EventLog, error) {
		return newDistributedLog(cfg)
	})
}

func newDistributedLog(cfg *eventlog.Config) (*distributedEventLog, error) {
	ww, err := veld.WatchWritableSegment(&cfg.VRN)
	if err != nil {
		return nil, err
	}

	rw, err := veld.WatchReadableSegments(&cfg.VRN)
	if err != nil {
		return nil, err
	}

	l := &distributedEventLog{
		cfg:             cfg,
		writableWatcher: ww,
		readableWatcher: rw,
		tracer: tracing.NewTracer("internal.eventlog.distributed",
			trace.SpanKindClient),
	}

	go func() {
		ch := ww.Chan()
		for {
			r, ok := <-ch
			if !ok {
				break
			}

			ctx, span := l.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if r != nil {
				l.updateWritableSegment(ctx, r)
			}

			l.writableWatcher.Wakeup()
			span.End()
		}
	}()
	ww.Start()

	go func() {
		ch := rw.Chan()
		for {
			rs, ok := <-ch
			if !ok {
				break
			}
			ctx, span := l.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if rs != nil {
				l.updateReadableSegments(ctx, rs)
			}

			l.readableWatcher.Wakeup()
			span.End()
		}
	}()
	rw.Start()

	return l, nil
}

type distributedEventLog struct {
	primitive.RefCount

	cfg *eventlog.Config

	writableWatcher *veld.WritableSegmentWatcher
	writableSegment *logSegment
	writableMu      sync.RWMutex

	readableWatcher  *veld.ReadableSegmentsWatcher
	readableSegments []*logSegment
	readableMu       sync.RWMutex
	tracer           *tracing.Tracer
}

// make sure distributedEventLog implements eventlog.EventLog.
var _ eventlog.EventLog = (*distributedEventLog)(nil)

func (l *distributedEventLog) VRN() *discovery.VRN {
	return &l.cfg.VRN
}

func (l *distributedEventLog) Close(ctx context.Context) {
	// TODO: stop discovery

	// TODO: lock
	if l.writableSegment != nil {
		l.writableSegment.Close(ctx)
	}
	for _, segment := range l.readableSegments {
		segment.Close(ctx)
	}
}

func (l *distributedEventLog) Writer() (eventlog.LogWriter, error) {
	w := &logWriter{
		elog: l,
	}
	l.Acquire()
	return w, nil
}

func (l *distributedEventLog) Reader(cfg eventlog.ReaderConfig) (eventlog.LogReader, error) {
	r := &logReader{
		elog: l,
		pos:  0,
		cfg:  cfg,
	}
	l.Acquire()
	return r, nil
}

func (l *distributedEventLog) updateWritableSegment(ctx context.Context, r *vdr.LogSegment) {
	if l.writableSegment != nil {
		if l.writableSegment.ID() == r.ID {
			_ = l.writableSegment.Update(ctx, r, true)
			return
		}
	}

	segment, err := newLogSegment(ctx, r, true)
	if err != nil {
		// TODO: create failed, to log
		return
	}

	l.writableMu.Lock()
	defer l.writableMu.Unlock()

	l.writableSegment = segment
}

func (l *distributedEventLog) selectWritableSegment(ctx context.Context) (*logSegment, error) {
	segment := l.fetchWritableSegment(ctx)
	if segment == nil {
		return nil, errors.ErrNotWritable
	}
	return segment, nil
}

func (l *distributedEventLog) fetchWritableSegment(ctx context.Context) *logSegment {
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

func (l *distributedEventLog) refreshWritableSegment(ctx context.Context) {
	_ = l.writableWatcher.Refresh(ctx)
}

func (l *distributedEventLog) updateReadableSegments(ctx context.Context, rs []*vdr.LogSegment) {
	segments := make([]*logSegment, 0, len(rs))
	for _, r := range rs {
		// TODO: find
		segment := func() *logSegment {
			for _, s := range l.readableSegments {
				if s.ID() == r.ID {
					return s
				}
			}
			return nil
		}()
		var err error
		if segment == nil {
			segment, err = newLogSegment(ctx, r, false)
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

func (l *distributedEventLog) selectReadableSegment(ctx context.Context, offset int64) (*logSegment, error) {
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

func (l *distributedEventLog) fetchReadableSegments(ctx context.Context) []*logSegment {
	l.readableMu.RLock()
	defer l.readableMu.RUnlock()

	if len(l.readableSegments) == 0 {
		// refresh
		func() {
			l.readableMu.RUnlock()
			defer l.readableMu.RLock()
			l.refreshReadableSegments(ctx)
		}()
	}

	return l.readableSegments
}

func (l *distributedEventLog) refreshReadableSegments(ctx context.Context) {
	_ = l.readableWatcher.Refresh(ctx)
}

// logWriter is the writer of distributedEventLog.
//
// Append is thread-safety.
type logWriter struct {
	elog *distributedEventLog
	cur  *logSegment
	mu   sync.RWMutex
}

func (w *logWriter) Log() eventlog.EventLog {
	return w.elog
}

func (w *logWriter) Close(ctx context.Context) {
	eventlog.Put(ctx, w.elog)
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

func (w *logWriter) selectWritableSegment(ctx context.Context) (*logSegment, error) {
	segment := func() *logSegment {
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
	elog *distributedEventLog
	pos  int64
	cur  *logSegment
	cfg  eventlog.ReaderConfig
}

func (r *logReader) Log() eventlog.EventLog {
	return r.elog
}

func (r *logReader) Close(ctx context.Context) {
	eventlog.Put(ctx, r.elog)
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
