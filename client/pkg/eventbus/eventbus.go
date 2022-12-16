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
	"sync"

	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/scylladb/go-set/u64set"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/policy"
	vlog "github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/pkg/errors"

	eb "github.com/linkall-labs/vanus/client/internal/vanus/eventbus"
	el "github.com/linkall-labs/vanus/client/internal/vanus/eventlog"
)

func NewEventbus(cfg *eb.Config) *eventbus {
	bus := &eventbus{
		cfg:            cfg,
		nameService:    eb.NewNameService(cfg.Endpoints),
		writableLogSet: u64set.New(),
		readableLogSet: u64set.New(),
		writableLogs:   make(map[uint64]eventlog.Eventlog, 0),
		readableLogs:   make(map[uint64]eventlog.Eventlog, 0),
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
				vlog.Debug(context.Background(), "eventbus quits writable watcher", map[string]interface{}{
					"eventbus": bus.cfg.Name,
				})
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
				vlog.Debug(context.Background(), "eventbus quits readable watcher", map[string]interface{}{
					"eventbus": bus.cfg.Name,
				})
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

type eventbus struct {
	cfg         *eb.Config
	nameService *eb.NameService

	writableWatcher *WritableLogsWatcher
	writableLogSet  *u64set.Set
	writableLogs    map[uint64]eventlog.Eventlog
	writableMu      sync.RWMutex
	writableState   error

	readableWatcher *ReadableLogsWatcher
	readableLogSet  *u64set.Set
	readableLogs    map[uint64]eventlog.Eventlog
	readableMu      sync.RWMutex
	readableState   error

	tracer *tracing.Tracer
}

// make sure eventbus implements EventBus.
var _ api.Eventbus = (*eventbus)(nil)

func (b *eventbus) defaultWriteOptions() *api.WriteOptions {
	return &api.WriteOptions{
		Oneway: false,
		Policy: policy.NewRoundRobinWritePolicy(b),
	}
}

func (b *eventbus) defaultReadOptions() *api.ReadOptions {
	return &api.ReadOptions{
		BatchSize:      1,
		PollingTimeout: api.DefaultPollingTimeout,
		Policy:         policy.NewRoundRobinReadPolicy(b, api.ConsumeFromWhereEarliest),
	}
}

func (b *eventbus) Writer(opts ...api.WriteOption) api.BusWriter {
	writeOpts := b.defaultWriteOptions()
	for _, opt := range opts {
		opt(writeOpts)
	}

	w := &busWriter{
		ebus:   b,
		opts:   writeOpts,
		tracer: tracing.NewTracer("pkg.eventbus.writer", trace.SpanKindClient),
	}
	return w
}

func (b *eventbus) Reader(opts ...api.ReadOption) api.BusReader {
	readOpts := b.defaultReadOptions()
	for _, opt := range opts {
		opt(readOpts)
	}

	r := &busReader{
		ebus:   b,
		opts:   readOpts,
		tracer: tracing.NewTracer("pkg.eventbus.reader", trace.SpanKindClient),
	}
	return r
}

func (b *eventbus) GetLog(ctx context.Context, logID uint64, opts ...api.LogOption) (api.Eventlog, error) {
	_, span := b.tracer.Start(ctx, "pkg.eventbus.getlog")
	defer span.End()
	op := &api.LogOptions{
		Policy: policy.NewReadOnlyPolicy(),
	}
	for _, opt := range opts {
		opt(op)
	}

	if op.Policy.AccessMode() == api.ReadOnly {
		if len(b.readableLogs) == 0 {
			b.refreshReadableLogs(ctx)
		}
		if log, ok := b.readableLogs[logID]; ok {
			return log, nil
		}
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	} else if op.Policy.AccessMode() == api.ReadWrite {
		if len(b.writableLogs) == 0 {
			b.refreshWritableLogs(ctx)
		}
		if log, ok := b.writableLogs[logID]; ok {
			return log, nil
		}
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	} else {
		return nil, errors.ErrUnknown.WithMessage("access mode not supported")
	}
}

func (b *eventbus) ListLog(ctx context.Context, opts ...api.LogOption) ([]api.Eventlog, error) {
	_, span := b.tracer.Start(ctx, "pkg.eventbus.listlog")
	defer span.End()
	op := &api.LogOptions{
		Policy: policy.NewReadOnlyPolicy(),
	}
	for _, opt := range opts {
		opt(op)
	}

	if op.Policy.AccessMode() == api.ReadOnly {
		if len(b.readableLogs) == 0 {
			b.refreshReadableLogs(ctx)
		}
		eventlogs := make([]api.Eventlog, 0)
		for _, el := range b.readableLogs {
			eventlogs = append(eventlogs, el)
		}
		return eventlogs, nil
	} else if op.Policy.AccessMode() == api.ReadWrite {
		if len(b.writableLogs) == 0 {
			b.refreshWritableLogs(ctx)
		}
		eventlogs := make([]api.Eventlog, 0)
		for _, el := range b.writableLogs {
			eventlogs = append(eventlogs, el)
		}
		return eventlogs, nil
	} else {
		return nil, errors.ErrUnknown.WithMessage("access mode not supported")
	}
}

func (b *eventbus) Name() string {
	return b.cfg.Name
}

func (b *eventbus) Close(ctx context.Context) {
	b.writableWatcher.Close()
	b.readableWatcher.Close()

	for _, w := range b.writableLogs {
		w.Close(ctx)
	}
	for _, r := range b.readableLogs {
		r.Close(ctx)
	}
}

func (b *eventbus) getWritableState() error {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()
	return b.writableState
}

func (b *eventbus) setWritableState(err error) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableState = err
}

func (b *eventbus) isNeedUpdateWritableLogs(err error) bool {
	if err == nil {
		b.setWritableState(nil)
		return true
	}
	if errors.Is(err, errors.ErrResourceNotFound) {
		b.setWritableState(err)
		return true
	}
	return false
}

func (b *eventbus) updateWritableLogs(ctx context.Context, re *WritableLogsResult) {
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
		return
	}

	removed := u64set.Difference(b.writableLogSet, s)
	added := u64set.Difference(s, b.writableLogSet)

	lws := make(map[uint64]eventlog.Eventlog, len(re.Eventlogs))
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
		log := eventlog.NewEventLog(cfg)
		lws[logID] = log
		return true
	})
	b.setWritableLogs(s, lws)
}

func (b *eventbus) setWritableLogs(s *u64set.Set, lws map[uint64]eventlog.Eventlog) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableLogSet = s
	b.writableLogs = lws
}

func (b *eventbus) getWritableLog(ctx context.Context, logID uint64) eventlog.Eventlog {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()

	if len(b.writableLogs) == 0 {
		func() {
			b.writableMu.RUnlock()
			defer b.writableMu.RLock()
			b.refreshWritableLogs(ctx)
		}()
	}

	return b.writableLogs[logID]
}

func (b *eventbus) refreshWritableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshWritableLogs")
	defer span.End()

	_ = b.writableWatcher.Refresh(_ctx)
}

func (b *eventbus) getReadableState() error {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()
	return b.readableState
}

func (b *eventbus) setReadableState(err error) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableState = err
}

func (b *eventbus) isNeedUpdateReadableLogs(err error) bool {
	if err == nil {
		b.setReadableState(nil)
		return true
	}
	if errors.Is(err, errors.ErrResourceNotFound) {
		b.setReadableState(err)
		return true
	}
	return false
}

func (b *eventbus) updateReadableLogs(ctx context.Context, re *ReadableLogsResult) {
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
		return
	}

	removed := u64set.Difference(b.readableLogSet, s)
	added := u64set.Difference(s, b.readableLogSet)

	lws := make(map[uint64]eventlog.Eventlog, len(re.Eventlogs))
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
		log := eventlog.NewEventLog(cfg)
		lws[logID] = log
		return true
	})
	b.setReadableLogs(s, lws)
}

func (b *eventbus) setReadableLogs(s *u64set.Set, lws map[uint64]eventlog.Eventlog) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableLogSet = s
	b.readableLogs = lws
}

func (b *eventbus) getReadableLog(ctx context.Context, logID uint64) eventlog.Eventlog {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()

	if len(b.readableLogs) == 0 {
		func() {
			b.readableMu.RUnlock()
			defer b.readableMu.RLock()
			b.refreshReadableLogs(ctx)
		}()
	}

	return b.readableLogs[logID]
}

func (b *eventbus) refreshReadableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshReadableLogs")
	defer span.End()

	_ = b.readableWatcher.Refresh(_ctx)
}

type busWriter struct {
	ebus   *eventbus
	opts   *api.WriteOptions
	tracer *tracing.Tracer
}

var _ api.BusWriter = (*busWriter)(nil)

func (w *busWriter) AppendOne(ctx context.Context, event *ce.Event, opts ...api.WriteOption) (eid string, err error) {
	_ctx, span := w.tracer.Start(ctx, "AppendOne")
	defer span.End()

	var writeOpts *api.WriteOptions = w.opts
	if len(opts) > 0 {
		writeOpts = w.opts.Copy()
		for _, opt := range opts {
			opt(writeOpts)
		}
	}

	// 1. pick a writer of eventlog
	lw, err := w.pickWritableLog(_ctx, writeOpts)
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

func (w *busWriter) SyncAppendOneStream(ctx context.Context, event *ce.Event, opts ...api.WriteOption) (eid string, err error) {
	_ctx, span := w.tracer.Start(ctx, "SyncAppendOneStream")
	defer span.End()

	var writeOpts *api.WriteOptions = w.opts
	if len(opts) > 0 {
		writeOpts = w.opts.Copy()
		for _, opt := range opts {
			opt(writeOpts)
		}
	}

	// 1. pick a writer of eventlog
	lw, err := w.pickWritableLog(_ctx, writeOpts)
	if err != nil {
		return "", err
	}

	// 2. append the event to the eventlog
	off, err := lw.SyncAppendStream(_ctx, event)
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

func (w *busWriter) AppendMany(ctx context.Context, events []*ce.Event, opts ...api.WriteOption) (eid string, err error) {
	// TODO(jiangkai): implement this method, by jiangkai, 2022.10.24
	return "", nil
}

func (w *busWriter) Bus() api.Eventbus {
	return w.ebus
}

func (w *busWriter) pickWritableLog(ctx context.Context, opts *api.WriteOptions) (eventlog.LogWriter, error) {
	_ctx, span := w.tracer.Start(ctx, "pickWritableLog")
	defer span.End()

	log, err := opts.Policy.NextLog(ctx)
	if err != nil {
		return nil, err
	}

	l := w.ebus.getWritableLog(_ctx, log.ID())
	if l == nil {
		return nil, stderrors.New("can not pick writable log")
	}

	return l.Writer(), nil
}

type busReader struct {
	ebus   *eventbus
	opts   *api.ReadOptions
	tracer *tracing.Tracer
}

var _ api.BusReader = (*busReader)(nil)

func (r *busReader) Read(ctx context.Context, opts ...api.ReadOption) ([]*ce.Event, int64, uint64, error) {
	_ctx, span := r.tracer.Start(ctx, "Read")
	defer span.End()

	var readOpts *api.ReadOptions = r.opts
	if len(opts) > 0 {
		readOpts = r.opts.Copy()
		for _, opt := range opts {
			opt(readOpts)
		}
	}

	// 1. pick a reader of eventlog
	lr, err := r.pickReadableLog(_ctx, readOpts)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// TODO(jiangkai): refactor eventlog interface to avoid seek every time, by jiangkai, 2022.10.24
	off, err := lr.Seek(_ctx, readOpts.Policy.Offset(), io.SeekStart)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// 2. read the event to the eventlog
	events, err := lr.Read(_ctx, int16(readOpts.BatchSize))
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}
	return events, off, lr.Log().ID(), nil
}

func (r *busReader) SyncReadStream(ctx context.Context, opts ...api.ReadOption) ([]*ce.Event, int64, uint64, error) {
	_ctx, span := r.tracer.Start(ctx, "Read")
	defer span.End()

	var readOpts *api.ReadOptions = r.opts
	if len(opts) > 0 {
		readOpts = r.opts.Copy()
		for _, opt := range opts {
			opt(readOpts)
		}
	}

	// 1. pick a reader of eventlog
	lr, err := r.pickReadableLog(_ctx, readOpts)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// TODO(jiangkai): refactor eventlog interface to avoid seek every time, by jiangkai, 2022.10.24
	off, err := lr.Seek(_ctx, readOpts.Policy.Offset(), io.SeekStart)
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}

	// 2. read the event to the eventlog
	events, err := lr.SyncReadStream(_ctx, int16(readOpts.BatchSize))
	if err != nil {
		return []*ce.Event{}, 0, 0, err
	}
	return events, off, lr.Log().ID(), nil
}

func (r *busReader) Bus() api.Eventbus {
	return r.ebus
}

func (r *busReader) pickReadableLog(ctx context.Context, opts *api.ReadOptions) (eventlog.LogReader, error) {
	_ctx, span := r.tracer.Start(ctx, "pickReadableLog")
	defer span.End()

	log, err := opts.Policy.NextLog(ctx)
	if err != nil {
		return nil, err
	}
	lr := r.ebus.getReadableLog(_ctx, log.ID())
	if lr == nil {
		return nil, stderrors.New("can not pick readable log")
	}

	return lr.Reader(eventlog.ReaderConfig{PollingTimeout: opts.PollingTimeout}), nil
}
