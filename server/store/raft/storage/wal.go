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

	// first-party libraries.
	vanus "github.com/vanus-labs/vanus/api/vsr"

	// this project.
	"github.com/vanus-labs/vanus/server/store/meta"
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

const (
	defaultCompactJobBufferSize = 256
)

// WAL wraps underlay walog.WAL and provisions compacting capability.
// All compact tasks be processed in WAL.runCompact by a single goroutine.
type WAL struct {
	*walog.WAL

	stateStore *meta.SyncStore

	nodes   map[vanus.ID]bool
	barrier *skiplist.SkipList

	compactC chan compactJob
	// closeMu prevents concurrent writing compactC and closing closeC.
	closeMu sync.RWMutex
	// closeC prevents writing to compactC after close.
	closeC chan struct{}
	doneC  chan struct{}
}

func newWAL(wal *walog.WAL, stateStore *meta.SyncStore, startCompaction bool) *WAL {
	w := &WAL{
		WAL:        wal,
		stateStore: stateStore,
		nodes:      make(map[vanus.ID]bool),
		barrier:    skiplist.New(skiplist.Int64),
		compactC:   make(chan compactJob, defaultCompactJobBufferSize),
		closeC:     make(chan struct{}),
		doneC:      make(chan struct{}),
	}

	if startCompaction {
		w.startCompaction()
	}

	return w
}

func (w *WAL) startCompaction() {
	go w.runCompact()
}

func (w *WAL) Close() {
	w.WAL.Close()

	go func() {
		w.WAL.Wait()

		w.closeMu.Lock()
		close(w.closeC)
		w.closeMu.Unlock()
	}()
}

func (w *WAL) Wait() {
	<-w.doneC
}
