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
	// third-party libraries.
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/observability/tracing"
	oteltracer "go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	defaultExecuteTaskBufferSize = 256
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

type executeCallback func() (compactTask, error)

type executeTask struct {
	cb     executeCallback
	result chan error
}

type WAL struct {
	*walog.WAL

	metaStore *meta.SyncStore

	barrier  *skiplist.SkipList
	executec chan executeTask
	compactc chan compactTask
	donec    chan struct{}
	tracer   *tracing.Tracer
}

func newWAL(wal *walog.WAL, metaStore *meta.SyncStore) *WAL {
	w := &WAL{
		WAL:       wal,
		metaStore: metaStore,
		barrier:   skiplist.New(skiplist.Int64),
		executec:  make(chan executeTask, defaultExecuteTaskBufferSize),
		compactc:  make(chan compactTask, defaultCompactTaskBufferSize),
		donec:     make(chan struct{}),
		tracer:    tracing.NewTracer("raft.log.wal", oteltracer.SpanKindInternal),
	}

	go w.run()
	go w.runCompact()

	return w
}

func (w *WAL) Close() {
	w.WAL.Close()
	go func() {
		w.WAL.Wait()
		close(w.executec)
	}()
}

func (w *WAL) Wait() {
	<-w.donec
}
