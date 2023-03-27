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
	"io"
	"sync"

	// third-party libraries.
	"github.com/scylladb/go-set/u64set"
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/tracing"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"

	// this project.
	eb "github.com/vanus-labs/vanus/client/internal/eventbus"
	el "github.com/vanus-labs/vanus/client/internal/eventlog"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/eventlog"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/client/pkg/primitive"
)

func NewEventbus(cfg *eb.Config, close api.CloseFunc) *Eventbus {
	bus := &Eventbus{
		cfg:            cfg,
		close:          close,
		nameService:    eb.NewNameService(cfg.Endpoints),
		writableLogSet: u64set.New(),
		readableLogSet: u64set.New(),
		writableLogs:   make(map[uint64]eventlog.Eventlog, 0),
		readableLogs:   make(map[uint64]eventlog.Eventlog, 0),
		writableMu:     sync.RWMutex{},
		readableMu:     sync.RWMutex{},
		writableState:  nil,
		readableState:  nil,
		RefCount:       primitive.RefCount{},
		tracer:         tracing.NewTracer("pkg.eventbus.impl", trace.SpanKindClient),
	}

	bus.writableWatcher = WatchWritableLogs(bus)
	bus.readableWatcher = WatchReadableLogs(bus)

	go func() {
		ch := bus.writableWatcher.Chan()
		for {
			re, ok := <-ch
			if !ok {
				log.Debug(context.Background(), "eventbus quits writable watcher", map[string]interface{}{
					"eventbus_id": bus.cfg.ID,
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
				log.Debug(context.Background(), "eventbus quits readable watcher", map[string]interface{}{
					"eventbus_id": bus.cfg.ID,
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

type Eventbus struct {
	cfg         *eb.Config
	close       api.CloseFunc
	nameService *eb.NameService
	closeOnce   sync.Once

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

	primitive.RefCount
	tracer *tracing.Tracer
}

// make sure eventbus implements api.Eventbus.
var _ api.Eventbus = (*Eventbus)(nil)

func (b *Eventbus) defaultWriteOptions() *api.WriteOptions {
	return &api.WriteOptions{
		Oneway: false,
		Policy: policy.NewRoundRobinWritePolicy(b),
	}
}

func (b *Eventbus) defaultReadOptions() *api.ReadOptions {
	return &api.ReadOptions{
		BatchSize:      1,
		PollingTimeout: api.DefaultPollingTimeout,
		Policy:         policy.NewRoundRobinReadPolicy(b, api.ConsumeFromWhereEarliest),
	}
}

func (b *Eventbus) Writer(opts ...api.WriteOption) api.BusWriter {
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

func (b *Eventbus) Reader(opts ...api.ReadOption) api.BusReader {
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

func (b *Eventbus) GetLog(ctx context.Context, logID uint64, opts ...api.LogOption) (api.Eventlog, error) {
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
		if l, ok := b.readableLogs[logID]; ok {
			return l, nil
		}
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	} else if op.Policy.AccessMode() == api.ReadWrite {
		if len(b.writableLogs) == 0 {
			b.refreshWritableLogs(ctx)
		}
		if l, ok := b.writableLogs[logID]; ok {
			return l, nil
		}
		return nil, errors.ErrResourceNotFound.WithMessage("eventlog not found")
	} else {
		return nil, errors.ErrUnknown.WithMessage("access mode not supported")
	}
}

func (b *Eventbus) ListLog(ctx context.Context, opts ...api.LogOption) ([]api.Eventlog, error) {
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

func (b *Eventbus) ID() uint64 {
	return b.cfg.ID
}

func (b *Eventbus) Close(ctx context.Context) {
	if b.Release() {
		func() {
			if b.UseCount() == 0 { // double check
				b.closeOnce.Do(func() {
					b.writableWatcher.Close()
					b.readableWatcher.Close()
					for _, w := range b.writableLogs {
						w.Close(ctx)
					}
					for _, r := range b.readableLogs {
						r.Close(ctx)
					}
					b.close(b.cfg.ID)
				})
			}
		}()
	}
}

func (b *Eventbus) getWritableState() error {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()
	return b.writableState
}

func (b *Eventbus) setWritableState(err error) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableState = err
}

func (b *Eventbus) isNeedUpdateWritableLogs(err error) bool {
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

func (b *Eventbus) updateWritableLogs(ctx context.Context, re *WritableLogsResult) {
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
		lws[logID] = eventlog.NewEventlog(cfg)
		return true
	})
	b.setWritableLogs(s, lws)
}

func (b *Eventbus) setWritableLogs(s *u64set.Set, lws map[uint64]eventlog.Eventlog) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableLogSet = s
	b.writableLogs = lws
}

func (b *Eventbus) getWritableLog(ctx context.Context, logID uint64) (eventlog.Eventlog, error) {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()

	if errors.Is(b.writableState, errors.ErrResourceNotFound) {
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
	}

	if len(b.writableLogs) == 0 {
		func() {
			b.writableMu.RUnlock()
			defer b.writableMu.RLock()
			b.refreshWritableLogs(ctx)
		}()
	}

	return b.writableLogs[logID], nil
}

func (b *Eventbus) refreshWritableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshWritableLogs")
	defer span.End()

	_ = b.writableWatcher.Refresh(_ctx)
}

func (b *Eventbus) getReadableState() error {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()
	return b.readableState
}

func (b *Eventbus) setReadableState(err error) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableState = err
}

func (b *Eventbus) isNeedUpdateReadableLogs(err error) bool {
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

func (b *Eventbus) updateReadableLogs(ctx context.Context, re *ReadableLogsResult) {
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
		lws[logID] = eventlog.NewEventlog(cfg)
		return true
	})
	b.setReadableLogs(s, lws)
}

func (b *Eventbus) setReadableLogs(s *u64set.Set, lws map[uint64]eventlog.Eventlog) {
	b.readableMu.Lock()
	defer b.readableMu.Unlock()
	b.readableLogSet = s
	b.readableLogs = lws
}

func (b *Eventbus) getReadableLog(ctx context.Context, logID uint64) (eventlog.Eventlog, error) {
	b.readableMu.RLock()
	defer b.readableMu.RUnlock()

	if errors.Is(b.readableState, errors.ErrResourceNotFound) {
		return nil, errors.ErrResourceNotFound.WithMessage("eventbus not found")
	}

	if len(b.readableLogs) == 0 {
		func() {
			b.readableMu.RUnlock()
			defer b.readableMu.RLock()
			b.refreshReadableLogs(ctx)
		}()
	}

	return b.readableLogs[logID], nil
}

func (b *Eventbus) refreshReadableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshReadableLogs")
	defer span.End()

	_ = b.readableWatcher.Refresh(_ctx)
}

type busWriter struct {
	ebus   *Eventbus
	opts   *api.WriteOptions
	tracer *tracing.Tracer
}

var _ api.BusWriter = (*busWriter)(nil)

func (w *busWriter) Append(ctx context.Context, events *cloudevents.CloudEventBatch, opts ...api.WriteOption) (eids []string, err error) {
	_ctx, span := w.tracer.Start(ctx, "Append")
	defer span.End()

	writeOpts := w.opts
	if len(opts) > 0 {
		writeOpts = w.opts.Copy()
		for _, opt := range opts {
			opt(writeOpts)
		}
	}

	// 1. pick a writer of eventlog
	lw, err := w.pickWritableLog(_ctx, writeOpts)
	if err != nil {
		log.Error(context.Background(), "pick writable log failed", map[string]interface{}{
			log.KeyError:  err,
			"eventbus_id": w.ebus.ID(),
		})
		return nil, err
	}

	// 2. append the event to the eventlog
	offsets, err := lw.Append(_ctx, events)
	if err != nil {
		log.Error(context.Background(), "logwriter append failed", map[string]interface{}{
			log.KeyError:  err,
			"eventbus_id": w.ebus.ID(),
			"eventlog_id": lw.Log().ID(),
		})
		return nil, err
	}

	eventIDs := make([]string, len(offsets))
	for idx := range offsets {
		eventIDs[idx] = genEventID(lw.Log().ID(), offsets[idx])
	}
	return eventIDs, nil
}

func (w *busWriter) Bus() api.Eventbus {
	return w.ebus
}

func (w *busWriter) pickWritableLog(ctx context.Context, opts *api.WriteOptions) (eventlog.LogWriter, error) {
	_ctx, span := w.tracer.Start(ctx, "pickWritableLog")
	defer span.End()

	l, err := opts.Policy.NextLog(ctx)
	if err != nil {
		return nil, err
	}

	lw, err := w.ebus.getWritableLog(_ctx, l.ID())
	if err != nil {
		return nil, err
	}
	if lw == nil {
		return nil, errors.ErrResourceCanNotOp.WithMessage("can not pick writable log")
	}

	return lw.Writer(), nil
}

func genEventID(logID uint64, off int64) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], logID)
	binary.BigEndian.PutUint64(buf[8:16], uint64(off))
	encoded := base64.StdEncoding.EncodeToString(buf[:])
	return encoded
}

type busReader struct {
	ebus   *Eventbus
	opts   *api.ReadOptions
	tracer *tracing.Tracer
}

var _ api.BusReader = (*busReader)(nil)

func (r *busReader) Read(ctx context.Context, opts ...api.ReadOption) (events *cloudevents.CloudEventBatch, off int64, logid uint64, err error) {
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
		log.Error(context.Background(), "pick readable log failed", map[string]interface{}{
			log.KeyError:  err,
			"eventbus_id": r.ebus.ID(),
		})
		return nil, 0, 0, err
	}

	// TODO(jiangkai): refactor eventlog interface to avoid seek every time, by jiangkai, 2022.10.24
	off, err = lr.Seek(_ctx, readOpts.Policy.Offset(), io.SeekStart)
	if err != nil {
		log.Error(context.Background(), "seek offset failed", map[string]interface{}{
			log.KeyError:  err,
			"eventbus_id": r.ebus.ID(),
		})
		return nil, 0, 0, err
	}

	// 2. read the event to the eventlog
	events, err = lr.Read(_ctx, int16(readOpts.BatchSize))
	if err != nil {
		return nil, 0, 0, err
	}
	return events, off, lr.Log().ID(), nil
}

func (r *busReader) Bus() api.Eventbus {
	return r.ebus
}

func (r *busReader) pickReadableLog(ctx context.Context, opts *api.ReadOptions) (eventlog.LogReader, error) {
	_ctx, span := r.tracer.Start(ctx, "pickReadableLog")
	defer span.End()

	l, err := opts.Policy.NextLog(ctx)
	if err != nil {
		return nil, err
	}
	lr, err := r.ebus.getReadableLog(_ctx, l.ID())
	if err != nil {
		return nil, err
	}
	if lr == nil {
		return nil, errors.ErrResourceCanNotOp.WithMessage("can not pick readable log")
	}

	return lr.Reader(eventlog.ReaderConfig{PollingTimeout: opts.PollingTimeout}), nil
}
