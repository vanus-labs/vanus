// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	// standard libraries.
	"context"
	"io"

	// third-party project.
	"go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

func (w *WAL) newAppender(ctx context.Context, entries [][]byte, callback AppendCallback) *appender {
	return &appender{
		w:        w,
		entries:  entries,
		ranges:   make([]Range, len(entries)),
		ctx:      ctx,
		callback: callback,
	}
}

type appender struct {
	w       *WAL
	entries [][]byte
	records []record.Record
	padding int
	i, j    int

	ranges []Range

	ctx      context.Context
	callback AppendCallback
}

// Make sure Data implements io.Reader.
var _ io.Reader = (*appender)(nil)

func (a *appender) invoke() {
	span := trace.SpanFromContext(a.ctx)
	span.AddEvent("store.wal.appender.invoke() Start")
	defer span.AddEvent("store.wal.appender.invoke() End")

	a.w.s.Append(a, a.onAppended)

	// metrics.WALEntryWriteCounter.Add(float64(len(entries)))
	// metrics.WALEntryWriteSizeCounter.Add(float64(entrySize))
	// metrics.WALRecordWriteCounter.Add(float64(recordCount))
	// metrics.WALRecordWriteSizeCounter.Add(float64(recordSize))

	// span.SetAttributes(
	// 	attribute.Int("entry_count", len(entries)),
	// 	attribute.Int("entry_size", entrySize),
	// 	attribute.Int("record_count", recordCount),
	// 	attribute.Int("record_size", recordSize))
}

func (a *appender) Read(b []byte) (int, error) {
	if a.j < len(a.records) {
		n, err := a.records[a.j].MarshalTo(b)
		a.j++
		return n, err
	}

	if a.padding != 0 {
		n := a.padding
		a.padding = 0
		return n, nil
	}

	if a.i != 0 {
		a.ranges[a.i-1].EO += a.w.s.WriteOffset()
	}

	if a.i < len(a.entries) {
		a.ranges[a.i].SO = a.w.s.WriteOffset()
		a.records, a.padding = record.Pack(a.entries[a.i], len(b), a.w.blockSize)
		a.ranges[a.i].EO = -int64(a.padding)
		a.i++
		a.j = 1
		return a.records[0].MarshalTo(b)
	}

	// Release memory.
	a.records = nil
	a.entries = nil

	return 0, io.EOF
}

func (a *appender) onAppended(_ int, err error) {
	if err != nil {
		panic(err)
	}

	a.w.callbackC <- callbackTask{
		ctx:      a.ctx,
		callback: a.callback,
		ranges:   a.ranges,
	}

	a.w.appendWg.Done()
}
