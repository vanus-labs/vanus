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
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"sync"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/scylladb/go-set/strset"
	"google.golang.org/grpc/status"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

func newEventBus(cfg *Config) EventBus {
	w, err := discovery.WatchWritableLogs(&cfg.VRN)
	if err != nil {
		return nil
	}

	bus := &basicEventBus{
		RefCount:        primitive.RefCount{},
		cfg:             cfg,
		writableWatcher: w,
		writableLogs:    strset.New(),
		logWriters:      make([]eventlog.LogWriter, 0),
		writableMu:      sync.RWMutex{},
		state:           nil,
		tracer:          tracing.NewTracer("pkg.eventbus.basic", trace.SpanKindClient),
	}

	go func() {
		ch := w.Chan()
		for {
			re, ok := <-ch
			if !ok {
				break
			}

			bus.updateWritableLogs(context.Background(), re)
			bus.writableWatcher.Wakeup()
		}
	}()
	w.Start()

	return bus
}

type basicEventBus struct {
	primitive.RefCount

	cfg *Config

	writableWatcher *discovery.WritableLogsWatcher
	writableLogs    *strset.Set
	logWriters      []eventlog.LogWriter
	writableMu      sync.RWMutex
	state           error
	tracer          *tracing.Tracer
}

// make sure basicEventBus implements EventBus.
var _ EventBus = (*basicEventBus)(nil)

func (b *basicEventBus) VRN() *discovery.VRN {
	return &b.cfg.VRN
}

func (b *basicEventBus) Close(ctx context.Context) {
	b.writableWatcher.Close()

	for _, w := range b.logWriters {
		w.Close(ctx)
	}
}

func (b *basicEventBus) Writer() (BusWriter, error) {
	w := &basicBusWriter{
		ebus:   b,
		picker: &RoundRobinPick{},
		tracer: tracing.NewTracer("pkg.eventbus.basic", trace.SpanKindClient),
	}
	b.Acquire()
	return w, nil
}

func (b *basicEventBus) getState() error {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()
	return b.state
}

func (b *basicEventBus) setState(err error) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.state = err
}

func (b *basicEventBus) isNeedUpdate(err error) bool {
	if err == nil {
		b.setState(nil)
		return true
	}
	sts := status.Convert(err)
	// TODO: temporary scheme, wait for error code reconstruction
	if strings.Contains(sts.Message(), "RESOURCE_NOT_FOUND") {
		b.setState(errors.ErrNotFound)
		return true
	}
	return false
}

func (b *basicEventBus) updateWritableLogs(ctx context.Context, re *discovery.WritableLogsResult) {
	_ctx, span := b.tracer.Start(ctx, "updateWritableLogs")
	defer span.End()

	if !b.isNeedUpdate(re.Err) {
		return
	}

	s := strset.NewWithSize(len(re.Eventlogs))
	for _, l := range re.Eventlogs {
		s.Add(l.VRN)
	}

	if b.writableLogs.IsEqual(s) {
		// no change
		return
	}

	// diff
	removed := strset.Difference(b.writableLogs, s)
	added := strset.Difference(s, b.writableLogs)

	a := make([]eventlog.LogWriter, 0, len(re.Eventlogs))
	for _, w := range b.logWriters {
		if !removed.Has(w.Log().VRN().String()) {
			a = append(a, w)
		} else {
			w.Close(ctx)
		}
	}
	added.Each(func(vrn string) bool {
		w, err := eventlog.OpenWriter(_ctx, vrn)
		if err != nil {
			// TODO: open failed, logging
			s.Remove(vrn)
		} else {
			a = append(a, w)
		}
		return true
	})
	// TODO: sort

	b.setWritableLogs(s, a)
}

func (b *basicEventBus) setWritableLogs(s *strset.Set, a []eventlog.LogWriter) {
	b.writableMu.Lock()
	defer b.writableMu.Unlock()
	b.writableLogs = s
	b.logWriters = a
}

func (b *basicEventBus) getLogWriters(ctx context.Context) []eventlog.LogWriter {
	b.writableMu.RLock()
	defer b.writableMu.RUnlock()

	if len(b.logWriters) == 0 {
		// refresh
		func() {
			b.writableMu.RUnlock()
			defer b.writableMu.RLock()
			b.refreshWritableLogs(ctx)
		}()
	}

	return b.logWriters
}

func (b *basicEventBus) refreshWritableLogs(ctx context.Context) {
	_ctx, span := b.tracer.Start(ctx, "refreshWritableLogs")
	defer span.End()

	_ = b.writableWatcher.Refresh(_ctx)
}

type basicBusWriter struct {
	ebus   *basicEventBus
	picker WriterPicker
	tracer *tracing.Tracer
}

func (w *basicBusWriter) Bus() EventBus {
	return w.ebus
}

func (w *basicBusWriter) Close(ctx context.Context) {
	Put(ctx, w.ebus)
}

func (w *basicBusWriter) Append(ctx context.Context, event *ce.Event) (string, error) {
	_ctx, span := w.tracer.Start(ctx, "Append")
	defer span.End()

	// 1. pick a writer of eventlog
	lw, err := w.pickLogWriter(_ctx, event)
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
	binary.BigEndian.PutUint64(buf[0:8], lw.Log().VRN().ID)
	binary.BigEndian.PutUint64(buf[8:16], uint64(off))
	encoded := base64.StdEncoding.EncodeToString(buf[:])

	return encoded, nil
}

func (w *basicBusWriter) pickLogWriter(ctx context.Context, event *ce.Event) (eventlog.LogWriter, error) {
	_ctx, span := w.tracer.Start(ctx, "pickLogWriter")
	defer span.End()

	lws := w.ebus.getLogWriters(_ctx)
	if len(lws) == 0 {
		if err := w.ebus.getState(); err != nil {
			return nil, err
		}
		return nil, errors.ErrNotWritable
	}

	lw := w.picker.Pick(event, lws)
	if lw == nil {
		return nil, stderrors.New("can not pick log writer")
	}

	return lw, nil
}

func (w *basicBusWriter) WithPicker(picker WriterPicker) BusWriter {
	w.picker = picker
	return w
}
