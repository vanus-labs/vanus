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
	"sync"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/scylladb/go-set/strset"

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
	}

	go func() {
		ch := w.Chan()
		for {
			re, ok := <-ch
			if !ok {
				break
			}

			bus.updateWritableLogs(re)
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
}

// make sure basicEventBus implements EventBus.
var _ EventBus = (*basicEventBus)(nil)

func (b *basicEventBus) VRN() *discovery.VRN {
	return &b.cfg.VRN
}

func (b *basicEventBus) Close() {
	b.writableWatcher.Close()

	for _, w := range b.logWriters {
		w.Close()
	}
}

func (b *basicEventBus) Writer() (BusWriter, error) {
	w := &basicBusWriter{
		ebus:   b,
		picker: &RoundRobinPick{},
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

func (b *basicEventBus) updateWritableLogs(re *discovery.WritableLogsResult) {
	b.setState(re.Err)

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
			w.Close()
		}
	}
	added.Each(func(vrn string) bool {
		w, err := eventlog.OpenWriter(vrn)
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
	_ = b.writableWatcher.Refresh(ctx)
}

type basicBusWriter struct {
	ebus   *basicEventBus
	picker WriterPicker
}

func (w *basicBusWriter) Bus() EventBus {
	return w.ebus
}

func (w *basicBusWriter) Close() {
	Put(w.ebus)
}

func (w *basicBusWriter) Append(ctx context.Context, event *ce.Event) (string, error) {
	// 1. pick a writer of eventlog
	lw, err := w.pickLogWriter(ctx, event)
	if err != nil {
		return "", err
	}

	// 2. append the event to the eventlog
	off, err := lw.Append(ctx, event)
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
	lws := w.ebus.getLogWriters(ctx)
	if len(lws) == 0 {
		if w.ebus.getState() != nil {
			return nil, w.ebus.getState()
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
