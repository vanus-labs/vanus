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

package log

import (
	// standard libraries.
	"sync"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/tracing"
	oteltrace "go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	defaultUpdateTaskBufferSize  = 256
	defaultCompactTaskBufferSize = 256
)

var walCompactKey = []byte("wal/compact")

type compactInfo struct {
	index, term uint64
}

type compactTask struct {
	offset, last int64
	nodeID       vanus.ID
	info         compactInfo
}

type reserveCallback func() (int64, error)

type WAL struct {
	*walog.WAL

	metaStore *meta.SyncStore

	barrier   *skiplist.SkipList
	updateC   chan compactTask
	compactC  chan compactTask
	compactMu sync.RWMutex
	doneC     chan struct{}
	tracer    *tracing.Tracer
}

func newWAL(wal *walog.WAL, metaStore *meta.SyncStore) *WAL {
	w := &WAL{
		WAL:       wal,
		metaStore: metaStore,
		barrier:   skiplist.New(skiplist.Int64),
		updateC:   make(chan compactTask, defaultUpdateTaskBufferSize),
		compactC:  make(chan compactTask, defaultCompactTaskBufferSize),
		doneC:     make(chan struct{}),
		tracer:    tracing.NewTracer("raft.log.wal", oteltrace.SpanKindInternal),
	}

	go w.runBarrierUpdate()
	go w.runCompact()

	return w
}

func (w *WAL) Close() {
	w.WAL.Close()
	go func() {
		w.WAL.Wait()
		close(w.updateC)
	}()
}

func (w *WAL) Wait() {
	<-w.doneC
}
