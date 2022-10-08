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

package eventbus

import (
	// standard libraries.
	"context"
	"encoding/base64"
	"encoding/binary"
	stderrors "errors"
	"io"
	"strings"
	"sync"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/status"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/scylladb/go-set/u64set"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/primitive"

	eb "github.com/linkall-labs/vanus/client/internal/vanus/eventbus"
	el "github.com/linkall-labs/vanus/client/internal/vanus/eventlog"
)

func NewEventBus(cfg *eb.Config) Eventbus {
	bus := &eventbusImpl{
		RefCount:       primitive.RefCount{},
		cfg:            cfg,
		nameService:    eb.NewNameService(cfg.Endpoints),
		writableLogSet: u64set.New(),
		readableLogSet: u64set.New(),
		writableLogs:   make(map[uint64]eventlog.EventlogImpl, 0),
		readableLogs:   make(map[uint64]eventlog.EventlogImpl, 0),
		writableMu:     sync.RWMutex{},
		readableMu:     sync.RWMutex{},
		writableState:  nil,
		readableState:  nil,
		tracer:         tracing.NewTracer("pkg.eventbus.impl", trace.SpanKindClient),
	}

	bus.writableWatcher = WatchWritableLogs(bus)
	bus.readableWatcher = WatchReadableLogs(bus)

	go func() {
		ch := bus.writableWatcher.Chan()
		for {
			re, ok := <-ch
			if !ok {
				break
			}

			ctx, span := bus.tracer.Start(context.Background(), "updateWritableLogsTask")
			if bus.writableWatcher != nil {
				bus.updateWritableLogs(ctx, re)
			}

			bus.writableWatcher.Wakeup()
			span.End()
		}
	}()
	bus.writableWatcher.Start()

	go func() {
		ch := bus.readableWatcher.Chan()
		for {
			re, ok := <-ch
			if !ok {
				break
			}

			ctx, span := bus.tracer.Start(context.Background(), "updateReadableLogsTask")
			if bus.readableWatcher != nil {
				bus.updateReadableLogs(ctx, re)
			}

			bus.readableWatcher.Wakeup()
			span.End()
		}
	}()
	bus.readableWatcher.Start()

	return bus
}

type eventbusImpl struct {
	primitive.RefCount

	cfg         *eb.Config
	nameService *eb.NameService

	writableWatcher *WritableLogsWatcher
	writableLogSet  *u64set.Set
	writableLogs    map[uint64]eventlog.EventlogImpl
	writableMu      sync.RWMutex
	writableState   error

	readableWatcher *ReadableLogsWatcher
	readableLogSet  *u64set.Set
	readableLogs    map[uint64]eventlog.EventlogImpl
	readableMu      sync.RWMutex
	readableState   error

	writeOpts *writeOptions
	readOpts  *readOptions
	tracer    *tracing.Tracer
}

// make sure eventbusImpl implements EventBus.
var _ Eventbus = (*eventbusImpl)(nil)

var defaultReadOptions = []ReadOption{
	WithPollingTimeout(DefaultPollingTimeout),
}

func (b *eventbusImpl) Writer(opts ...WriteOption) BusWriter {
	b.writeOpts = &writeOptions{policy: NewRoundRobinWritePolicy(b)}
	for _, opt := range opts {
		opt(b.writeOpts)
	}

	w := &busWriter{
		ebus:   b,
		tracer: tracing.NewTracer("pkg.eventbus.writer", trace.SpanKindClient),
	}
	return w
}

func (b *eventbusImpl) Reader(opts ...ReadOption) BusReader {
	ls, err := b.ListLog(context.Background())
	if err != nil {
		return nil
	}
	b.readOpts = &readOptions{policy: NewRoundRobinReadPolicy(ls[0], ConsumeFromWhereEarliest)}
	for _, opt := range defaultReadOptions {
		opt(b.readOpts)
	}

	for _, opt := range opts {
		opt(b.readOpts)
	}

	r := &busReader{
		ebus:   b,
		tracer: tracing.NewTracer("pkg.eventbus.reader", trace.SpanKindClient),
	}
	return r
}

func (b *eventbusImpl) GetLog(ctx context.Context, logID uint64, opts ...LogOption) (eventlog.Eventlog, error) {
	_, span := b.tracer.Start(ctx, "pkg.eventbus.getlog")
	defer span.End()
	op := &logOptions{
		policy: NewReadOnlyPolicy(),
	}
	for _, opt := range opts {
		opt(op)
	}

	if op.policy.AccessMode() == ReadOnly {
		if len(b.readableLogs) == 0 {
			b.refreshReadableLogs(ctx)
		}
		if log, ok := b.readableLogs[logID]; ok {
			return log, nil
		}
		return nil, errors.ErrNotFound
	} else if op.policy.AccessMode() == ReadWrite {
		if len(b.writableLogs) == 0 {
			b.refreshWritableLogs(ctx)
		}
		if log, ok := b.writableLogs[logID]; ok {
			return log, nil
		}
		return nil, errors.ErrNotFound
	} else {
		return nil, errors.ErrUnknown
	}
}

func (b *eventbusImpl) ListLog(ctx context.Context, opts ...LogOption) ([]eventlog.Eventlog, error) {
	_, span := b.tracer.Start(ctx, "pkg.eventbus.listlog")
	defer span.End()
	op := &logOptions{
		policy: NewReadOnlyPolicy(),
	}
	for _, opt := range opts {
		opt(op)
	}

	if op.policy.AccessMode() == ReadOnly {
		if len(b.readableLogs) == 0 {
			b.refreshReadableLogs(ctx)
		}
		eventlogs := make([]eventlog.Eventlog, 0)
		for _, el := range b.readableLogs {
			eventlogs = append(eventlogs, el)
		}
		return eventlogs, nil
	} else if op.policy.AccessMode() == ReadWrite {
		if len(b.writableLogs) == 0 {
			b.refreshWritableLogs(ctx)
		}
		eventlogs := make([]eventlog.Eventlog, 0)
		for _, el := range b.writableLogs {
			eventlogs = append(eventlogs, el)
		}
		return eventlogs, nil
	} else {
		return nil, errors.ErrUnknown
	}
}

func (b *eventbusImpl) Name() string {
	return b.cfg.Name
}

func (b *eventbusImpl) getWritableState() error {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()
	return b.writableState
}

func (b *eventbusImpl) setWritableState(err error) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableState = err
}

func (b *eventbusImpl) isNeedUpdateWritableLogs(err error) bool {
	if err == nil {
		b.setWritableState(nil)
		return true
	}
	sts := status.Convert(err)
	// TODO: temporary scheme, wait for error code reconstruction
	if strings.Contains(sts.Message(), "RESOURCE_NOT_FOUND") {
		b.setWritableState(errors.ErrNotFound)
		return true
	}
	return false
}

func (b *eventbusImpl) updateWritableLogs(ctx context.Context, re *WritableLogsResult) {
	_, span := b.tracer.Start(ctx, "updateWritableLogs")
	defer span.End()

	if !b.isNeedUpdateWritableLogs(re.Err) {
		return
	}

	s := u64set.NewWithSize(len(re.Eventlogs))
	for _, l := range re.Eventlogs {
		s.Add(l.ID)
	}

	if b.writableLogSet.IsEqual(s) {
		// no change
		return
	}

	// diff
	removed := u64set.Difference(b.writableLogSet, s)
	added := u64set.Difference(s, b.writableLogSet)

	lws := make(map[uint64]eventlog.EventlogImpl, len(re.Eventlogs))
	for id, lw := range b.writableLogs {
		if !removed.Has(id) {
			lws[id] = lw
		} else {
			lw.Close(ctx)
		}
	}
	added.Each(func(logID uint64) bool {
		cfg := &el.Config{
			Endpoints: b.cfg.Endpoints,
			ID:        logID,
		}
		log := eventlog.NewEventLogImpl(cfg)
		lws[logID] = log
		return true
	})
	// TODO: sort

	b.setWritableLogs(s, lws)
}

func (b *eventbusImpl) setWritableLogs(s *u64set.Set, lws map[uint64]eventlog.EventlogImpl) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableLogSet = s
	b.writableLogs = lws
}

func (b *eventbusImpl) getWritableLog(ctx context.Context) eventlog.EventlogImpl {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()

	if len(b.writableLogs) == 0 {
		// refresh
		func() {
			b.writableMu.RUnlock()
			defer b.writableMu.RLock()
			b.refreshWritableLogs(ctx)
		}()
	}

	el, err := b.writeOpts.policy.NextLog(ctx)
	if err != nil {
		return nil
	}

	return b.writableLogs[el.ID()]
}

func (b *eventbusImpl) refreshWritableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshWritableLogs")
	defer span.End()

	_ = b.writableWatcher.Refresh(_ctx)
}

func (b *eventbusImpl) getReadableState() error {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()
	return b.readableState
}

func (b *eventbusImpl) setReadableState(err error) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableState = err
}

func (b *eventbusImpl) isNeedUpdateReadableLogs(err error) bool {
	if err == nil {
		b.setReadableState(nil)
		return true
	}
	sts := status.Convert(err)
	// TODO: temporary scheme, wait for error code reconstruction
	if strings.Contains(sts.Message(), "RESOURCE_NOT_FOUND") {
		b.setReadableState(errors.ErrNotFound)
		return true
	}
	return false
}

func (b *eventbusImpl) updateReadableLogs(ctx context.Context, re *ReadableLogsResult) {
	_, span := b.tracer.Start(ctx, "updateReadableLogs")
	defer span.End()

	if !b.isNeedUpdateReadableLogs(re.Err) {
		return
	}

	s := u64set.NewWithSize(len(re.Eventlogs))
	for _, l := range re.Eventlogs {
		s.Add(l.ID)
	}

	if b.readableLogSet.IsEqual(s) {
		// no change
		return
	}

	// diff
	removed := u64set.Difference(b.readableLogSet, s)
	added := u64set.Difference(s, b.readableLogSet)

	lws := make(map[uint64]eventlog.EventlogImpl, len(re.Eventlogs))
	for id, lw := range b.readableLogs {
		if !removed.Has(id) {
			lws[id] = lw
		} else {
			lw.Close(ctx)
		}
	}
	added.Each(func(logID uint64) bool {
		cfg := &el.Config{
			Endpoints: b.cfg.Endpoints,
			ID:        logID,
		}
		log := eventlog.NewEventLogImpl(cfg)
		lws[logID] = log
		return true
	})
	// TODO: sort

	b.setReadableLogs(s, lws)
}

func (b *eventbusImpl) setReadableLogs(s *u64set.Set, lws map[uint64]eventlog.EventlogImpl) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableLogSet = s
	b.readableLogs = lws
}

func (b *eventbusImpl) getReadableLog(ctx context.Context) eventlog.EventlogImpl {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()

	if len(b.readableLogs) == 0 {
		// refresh
		func() {
			b.readableMu.RUnlock()
			defer b.readableMu.RLock()
			b.refreshReadableLogs(ctx)
		}()
	}

	el, err := b.readOpts.policy.NextLog(ctx)
	if err != nil {
		return nil
	}

	return b.readableLogs[el.ID()]
}

func (b *eventbusImpl) refreshReadableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshReadableLogs")
	defer span.End()

	_ = b.readableWatcher.Refresh(_ctx)
}

type busWriter struct {
	ebus   *eventbusImpl
	tracer *tracing.Tracer
}

var _ BusWriter = (*busWriter)(nil)

func (w *busWriter) AppendOne(ctx context.Context, event *ce.Event, opts ...WriteOption) (eid string, err error) {
	_ctx, span := w.tracer.Start(ctx, "AppendOne")
	defer span.End()

	for _, opt := range opts {
		opt(w.ebus.writeOpts)
	}

	// 1. pick a writer of eventlog
	lw, err := w.pickWritableLog(_ctx)
	if err != nil {
		return "", err
	}

	// 2. append the event to the eventlog
	off, err := lw.Append(_ctx, event)
	if err != nil {
		return "", err
	}

	// 3. generate event ID
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], lw.Log().ID())
	binary.BigEndian.PutUint64(buf[8:16], uint64(off))
	encoded := base64.StdEncoding.EncodeToString(buf[:])

	return encoded, nil
}

func (w *busWriter) AppendMany(ctx context.Context, events []*ce.Event, opts ...WriteOption) (eid string, err error) {
	// TODO(jiangkai): implement this method, by jiangkai, 2022.10.24
	return "", nil
}

func (w *busWriter) Bus() Eventbus {
	return w.ebus
}

func (w *busWriter) pickWritableLog(ctx context.Context) (eventlog.LogWriter, error) {
	_ctx, span := w.tracer.Start(ctx, "pickWritableLog")
	defer span.End()

	l := w.ebus.getWritableLog(_ctx)
	if l == nil {
		return nil, stderrors.New("can not pick writable log")
	}

	return l.Writer(), nil
}

type busReader struct {
	ebus   *eventbusImpl
	tracer *tracing.Tracer
}

var _ BusReader = (*busReader)(nil)

func (r *busReader) Read(ctx context.Context, size int16, opts ...ReadOption) ([]*ce.Event, int64, uint64, error) {
	_ctx, span := r.tracer.Start(ctx, "Read")
	defer span.End()

	for _, opt := range opts {
		opt(r.ebus.readOpts)
	}

	// 1. pick a reader of eventlog
	lr, err := r.pickReadableLog(_ctx)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// TODO(jiangkai): refactor eventlog interface to avoid seek every time, by jiangkai, 2022.10.24
	off, err := lr.Seek(_ctx, r.ebus.readOpts.policy.Offset(), io.SeekStart)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// 2. read the event to the eventlog
	events, err := lr.Read(_ctx, size)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}
	return events, off, lr.Log().ID(), nil
}

func (r *busReader) Bus() Eventbus {
	return r.ebus
}

func (r *busReader) pickReadableLog(ctx context.Context) (eventlog.LogReader, error) {
	_ctx, span := r.tracer.Start(ctx, "pickReadableLog")
	defer span.End()

	lr := r.ebus.getReadableLog(_ctx)
	if lr == nil {
		return nil, stderrors.New("can not pick readable log")
	}

	return lr.Reader(eventlog.ReaderConfig{PollingTimeout: r.ebus.readOpts.pollingTimeout}), nil
}
