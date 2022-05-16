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

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

var (
	walCompactKey = []byte("wal/compact")
)

type compactInfo struct {
	index, term uint64
}

type compactTask struct {
	offset, last int64
	nodeID       vanus.ID
	info         compactInfo
}

type exeCallback func() (compactTask, error)

type exeTask struct {
	cb     exeCallback
	result chan error
}

type WAL struct {
	*walog.WAL

	metaStore *meta.SyncStore

	barrier  *skiplist.SkipList
	exec     chan exeTask
	compactc chan compactTask
}

func newWAL(wal *walog.WAL, metaStore *meta.SyncStore) *WAL {
	w := &WAL{
		WAL:       wal,
		metaStore: metaStore,
		barrier:   skiplist.New(skiplist.Int64),
		exec:      make(chan exeTask, 256),
		compactc:  make(chan compactTask, 256),
	}

	go w.run()
	go w.runCompact()

	return w
}
