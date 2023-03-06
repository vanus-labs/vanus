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

package storage

import (
	// standard libraries.
	"sync"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/store/meta"
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

const (
	defaultCompactTaskBufferSize = 256
)

type WAL struct {
	*walog.WAL

	stateStore *meta.SyncStore

	nodes    map[vanus.ID]bool
	barrier  *skiplist.SkipList
	compactC chan func(*WAL, *compactContext)

	closeMu sync.RWMutex
	closeC  chan struct{}
	doneC   chan struct{}
}

func newWAL(wal *walog.WAL, stateStore *meta.SyncStore) *WAL {
	w := &WAL{
		WAL:        wal,
		stateStore: stateStore,
		nodes:      make(map[vanus.ID]bool),
		barrier:    skiplist.New(skiplist.Int64),
		compactC:   make(chan func(*WAL, *compactContext), defaultCompactTaskBufferSize),
		closeC:     make(chan struct{}),
		doneC:      make(chan struct{}),
	}

	go w.runCompact()

	return w
}

func (w *WAL) Close() {
	w.WAL.Close()
	go func() {
		w.WAL.Wait()

		w.closeMu.Lock()
		defer w.closeMu.Unlock()
		close(w.closeC)
	}()
}

func (w *WAL) Wait() {
	<-w.doneC
}
