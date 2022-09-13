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

package inmemory

import (
	// standard libraies.
	"context"
	"encoding/binary"
	"io"
	"strconv"
	"sync"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/primitive"
)

// func init() {
// 	UseInMemoryLog("inmemory")
// }

func UseInMemoryLog(scheme string) {
	eventlog.Register(scheme, func(cfg *eventlog.Config) (eventlog.EventLog, error) {
		return NewInMemoryLog(cfg), nil
	})
}

func NewInMemoryLog(cfg *eventlog.Config) *EventLog {
	l := &EventLog{
		cfg:    cfg,
		start:  0,
		events: []*ce.Event{},
		mu:     sync.RWMutex{},
	}

	// keepalive
	ka, ok := cfg.Attrs["keepalive"]
	if ok {
		on, err := strconv.ParseBool(ka[0])
		if err == nil && on {
			l.Acquire()
		}
	}

	return l
}

// NOTE: The elements of `events` field are mutable.
type EventLog struct {
	primitive.RefCount

	cfg *eventlog.Config

	start  int64
	events []*ce.Event
	mu     sync.RWMutex
}

func (l *EventLog) VRN() *discovery.VRN {
	return &l.cfg.VRN
}

func (l *EventLog) Close(ctx context.Context) {
}

func (l *EventLog) Writer() (eventlog.LogWriter, error) {
	w := &logWriter{
		elog: l,
	}
	l.Acquire()
	return w, nil
}

func (l *EventLog) Reader(_ eventlog.ReaderConfig) (eventlog.LogReader, error) {
	r := &logReader{
		elog: l,
		pos:  0,
	}
	l.Acquire()
	return r, nil
}

func (l *EventLog) Truncate(offset int64) error {
	// TODO
	return nil
}

type logWriter struct {
	elog *EventLog
}

func (w *logWriter) Log() eventlog.EventLog {
	return w.elog
}

func (w *logWriter) Close(ctx context.Context) {
	eventlog.Put(ctx, w.elog)
}

func (w *logWriter) Append(ctx context.Context, event *ce.Event) (int64, error) {
	elog := w.elog

	elog.mu.Lock()
	defer elog.mu.Unlock()

	offset := elog.start + int64(len(elog.events))
	elog.events = append(elog.events, event)

	return offset, nil
}

type logReader struct {
	elog *EventLog
	pos  int64
}

func (r *logReader) Log() eventlog.EventLog {
	return r.elog
}

func (r *logReader) Close(ctx context.Context) {
	eventlog.Put(ctx, r.elog)
}

func (r *logReader) Read(ctx context.Context, size int16) ([]*ce.Event, error) {
	elog := r.elog

	if size < 0 {
		return nil, errors.ErrInvalidArgument
	} else if size == 0 {
		return []*ce.Event{}, nil
	}

	elog.mu.RLock()
	defer elog.mu.RUnlock()

	length := int64(len(elog.events))
	si := r.pos - elog.start

	if si < 0 {
		return nil, errors.ErrUnderflow
	} else if si > length {
		return nil, errors.ErrOverflow
	} else if si == length {
		// no new events
		return []*ce.Event{}, nil
	}

	ei := si + int64(size)
	if ei > length {
		ei = length
	}
	events := elog.events[si:ei]
	for i := range events {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(si+int64(i)))
		events[i].SetExtension(eventlog.XVanusLogOffset, buf)
	}
	r.pos = elog.start + ei

	return events, nil
}

func (r *logReader) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		if offset == 0 {
			// return current position
			return r.pos, nil
		}
		fallthrough
	case io.SeekStart, io.SeekEnd:
		r.elog.mu.RLock()
		defer r.elog.mu.RUnlock()

		start := r.elog.start
		end := r.elog.start + int64(len(r.elog.events))
		switch whence {
		case 1:
			offset = r.pos + offset
		case 2:
			offset = end + offset
		}

		return r.doSeek(offset, start, end)
	}

	return -1, errors.ErrInvalidArgument
}

func (r *logReader) doSeek(offset, start, end int64) (int64, error) {
	if offset < start {
		return -1, errors.ErrUnderflow
	} else if offset > end {
		return -1, errors.ErrOverflow
	}
	r.pos = offset
	return offset, nil
}
