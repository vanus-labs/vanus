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
	"encoding/base64"
	"encoding/binary"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	elns "github.com/linkall-labs/vanus/client/internal/vanus/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/record"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/errors"
)

const (
	defaultRetryTimes = 10
	pollingThreshold  = 200 // in milliseconds.
	pollingPostSpan   = 100 // in milliseconds.
)

func NewEventLog(cfg *elns.Config) Eventlog {
	el := &eventlog{
		cfg:              cfg,
		nameService:      elns.NewNameService(cfg.Endpoints),
		writableSegments: make(map[uint64]*segment, 0),
		readableSegments: make(map[uint64]*segment, 0),
		tracer: tracing.NewTracer("pkg.eventlog.impl",
			trace.SpanKindClient),
	}

	el.writableWatcher = WatchWritableSegment(el)
	el.readableWatcher = WatchReadableSegments(el)

	go func() {
		ch := el.writableWatcher.Chan()
		for {
			rs, ok := <-ch
			if !ok {
				log.Debug(context.Background(), "eventlog quits writable watcher", map[string]interface{}{
					"eventlog": el.cfg.ID,
				})
				break
			}

			ctx, span := el.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if rs != nil {
				el.updateWritableSegment(ctx, rs)
			}

			el.writableWatcher.Wakeup()
			span.End()
		}
	}()
	el.writableWatcher.Start()

	go func() {
		ch := el.readableWatcher.Chan()
		for {
			rs, ok := <-ch
			if !ok {
				log.Debug(context.Background(), "eventlog quits readable watcher", map[string]interface{}{
					"eventlog": el.cfg.ID,
				})
				break
			}
			ctx, span := el.tracer.Start(context.Background(), "updateReadableSegmentsTask")
			if rs != nil {
				el.updateReadableSegments(ctx, rs)
			}

			el.readableWatcher.Wakeup()
			span.End()
		}
	}()
	el.readableWatcher.Start()

	return el
}

type eventlog struct {
	cfg         *elns.Config
	nameService *elns.NameService

	writableWatcher  *WritableSegmentWatcher
	writableSegments map[uint64]*segment
	writableMu       sync.RWMutex
	logWriter        *logWriter
	writerMu         sync.RWMutex

	readableWatcher  *ReadableSegmentsWatcher
	readableSegments map[uint64]*segment
	readableMu       sync.RWMutex
	logReader        *logReader
	readerMu         sync.RWMutex
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
	l.logWriter = nil
	l.logReader = nil

	for _, segment := range l.writableSegments {
		segment.Close(ctx)
	}
	for _, segment := range l.readableSegments {
		segment.Close(ctx)
	}
}

func (l *eventlog) Writer() LogWriter {
	if l.logWriter != nil {
		return l.logWriter
	}
	l.writerMu.Lock()
	defer l.writerMu.Unlock()
	if l.logWriter != nil {
		return l.logWriter
	}
	l.logWriter = &logWriter{
		elog: l,
	}
	return l.logWriter
}

func (l *eventlog) Reader(cfg ReaderConfig) LogReader {
	if l.logReader != nil {
		return l.logReader
	}
	l.readerMu.Lock()
	defer l.readerMu.Unlock()
	if l.logWriter != nil {
		return l.logReader
	}
	l.logReader = &logReader{
		elog: l,
		pos:  0,
		cfg:  cfg,
	}
	return l.logReader
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

	if len(segs) == 0 {
		return -1, nil
	}

	if segs[0].firstEventBornAt.After(t) {
		return segs[0].startOffset, nil
	}

	tailSeg := fetchTailSegment(ctx, segs)
	if tailSeg.lastEventBornAt.Before(t) {
		// the target offset maybe in newer segment, refresh immediately
		l.refreshReadableSegments(ctx)
		segs = l.fetchReadableSegments(ctx)
	}

	for idx := range segs {
		seg := segs[idx]
		if !t.Before(seg.firstEventBornAt) && !t.After(seg.lastEventBornAt) {
			target = seg
			break
		}
	}

	if target == nil {
		target = tailSeg
	}

	return target.LookupOffset(ctx, t)
}

func (l *eventlog) updateWritableSegment(ctx context.Context, rs []*record.Segment) {
	segments := make(map[uint64]*segment, len(rs))
	for _, r := range rs {
		// TODO: find
		segment := func() *segment {
			for _, s := range l.writableSegments {
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
		segments[segment.id] = segment
	}

	l.writableMu.Lock()
	defer l.writableMu.Unlock()

	l.writableSegments = segments
}

func (l *eventlog) selectWritableSegment(ctx context.Context) (*segment, error) {
	segment := l.fetchWritableSegment(ctx)
	if segment == nil {
		return nil, errors.ErrNotWritable
	}
	return segment, nil
}

func (l *eventlog) nextWritableSegment(ctx context.Context, seg *segment) (*segment, error) {
	l.writableMu.RLock()
	defer l.writableMu.RUnlock()
	if s, ok := l.writableSegments[seg.nextSegmentId]; ok {
		return s, nil
	}
	return nil, errors.ErrResourceNotFound
}

func (l *eventlog) fetchWritableSegment(ctx context.Context) *segment {
	l.writableMu.RLock()
	defer l.writableMu.RUnlock()

	if len(l.writableSegments) == 0 {
		// refresh
		func() {
			l.writableMu.RUnlock()
			defer l.writableMu.RLock()
			l.refreshWritableSegment(ctx)
		}()
	}

	// fetch the front segment by previousSegmentId
	for _, segment := range l.writableSegments {
		if _, ok := l.writableSegments[segment.previousSegmentId]; !ok {
			return segment
		}
	}

	return nil
}

func (l *eventlog) refreshWritableSegment(ctx context.Context) {
	_ = l.writableWatcher.Refresh(ctx)
}

func (l *eventlog) updateReadableSegments(ctx context.Context, rs []*record.Segment) {
	segments := make(map[uint64]*segment, len(rs))
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
		segments[segment.id] = segment
	}

	l.readableMu.Lock()
	defer l.readableMu.Unlock()

	l.readableSegments = segments
}

func (l *eventlog) selectReadableSegment(ctx context.Context, offset int64) (*segment, error) {
	segs := l.fetchReadableSegments(ctx)
	if len(segs) == 0 {
		return nil, errors.ErrNotReadable
	}
	var target *segment
	target = fetchTailSegment(ctx, segs)
	if offset == target.EndOffset() {
		return nil, errors.ErrOffsetOnEnd
	}
	if offset > target.EndOffset() {
		return nil, errors.ErrOffsetOverflow
	}

	target = fetchHeadSegment(ctx, segs)
	if offset < target.StartOffset() {
		return nil, errors.ErrOffsetUnderflow
	}

	segmentNum := len(l.readableSegments)
	n := sort.Search(segmentNum, func(i int) bool {
		return l.readableSegments[uint64(i)].EndOffset() > offset
	})
	if n < segmentNum {
		return l.readableSegments[uint64(n)], nil
	}
	return nil, errors.ErrNotReadable
}

func (l *eventlog) fetchReadableSegments(ctx context.Context) map[uint64]*segment {
	l.readableMu.RLock()
	defer l.readableMu.RUnlock()

	if len(l.readableSegments) == 0 {
		l.readableMu.RUnlock()
		// refresh
		l.refreshReadableSegments(ctx)
		l.readableMu.RLock()
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

func (w *logWriter) Append(ctx context.Context, event *ce.Event) (string, error) {
	// TODO: async for throughput
	retryTimes := defaultRetryTimes
	for i := 1; i <= retryTimes; i++ {
		eid, err := w.doAppend(ctx, event)
		if err == nil {
			return eid, nil
		}
		log.Warning(ctx, "failed to append", map[string]interface{}{
			log.KeyError: err,
			"retryTimes": i,
		})
		if errors.Is(err, errors.ErrSegmentFull) {
			w.switchNextWritableSegment(ctx)
			if i < retryTimes {
				continue
			}
		}
		return "", err
	}
	return "", errors.ErrUnknown
}

func (w *logWriter) doAppend(ctx context.Context, event *ce.Event) (string, error) {
	segment, err := w.selectWritableSegment(ctx)
	if err != nil {
		return "", err
	}
	offset, err := segment.Append(ctx, event)
	if err != nil {
		if errors.Is(err, errors.ErrSegmentFull) {
			segment.SetNotWritable()
		}
		return "", err
	}
	return w.generateEventID(segment, offset), nil
}

func (w *logWriter) generateEventID(s *segment, offset int64) string {
	var buf [32]byte
	binary.BigEndian.PutUint64(buf[0:8], s.id)
	binary.BigEndian.PutUint64(buf[8:16], uint64(offset))
	binary.BigEndian.PutUint64(buf[16:24], w.elog.ID())
	binary.BigEndian.PutUint64(buf[24:32], uint64(offset+s.startOffset))
	return base64.StdEncoding.EncodeToString(buf[:])
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

func (w *logWriter) switchNextWritableSegment(ctx context.Context) {
	nextSeg, err := w.elog.nextWritableSegment(ctx, w.cur)
	if err != nil {
		return
	}
	w.cur = nextSeg
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
		if errors.Is(err, errors.ErrOffsetOnEnd) {
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
		if errors.Is(err, errors.ErrOffsetOverflow) {
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

func fetchHeadSegment(ctx context.Context, segments map[uint64]*segment) *segment {
	for _, segment := range segments {
		if _, ok := segments[segment.previousSegmentId]; !ok {
			return segment
		}
	}
	return nil
}

func fetchTailSegment(ctx context.Context, segments map[uint64]*segment) *segment {
	for _, segment := range segments {
		if _, ok := segments[segment.nextSegmentId]; !ok {
			return segment
		}
	}
	return nil
}
