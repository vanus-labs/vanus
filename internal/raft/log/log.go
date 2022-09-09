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
	"context"
	"fmt"
	"sync"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
)

type Log struct {
	// Protects access to all fields. Most methods of Log are
	// run on the raft goroutine, but Append() and Compact() is run on an
	// application goroutine.
	sync.RWMutex

	nodeID vanus.ID

	stateStorage
	logStorage
	snapshotStorage
}

// Make sure Log implements raft.Storage.
var _ raft.Storage = (*Log)(nil)

// NewLog creates an empty Log.
func NewLog(
	nodeID vanus.ID, wal *WAL, metaStore *meta.SyncStore, offsetStore *meta.AsyncStore, snapOp SnapshotOperator,
) *Log {
	return &Log{
		nodeID: nodeID,
		logStorage: logStorage{
			// When starting from scratch populate the list with a dummy entry at term zero.
			ents: make([]raftpb.Entry, 1),
			offs: make([]int64, 1),
			wal:  wal,
		},
		stateStorage: stateStorage{
			metaStore:   metaStore,
			offsetStore: offsetStore,
			hsKey:       []byte(fmt.Sprintf("block/%020d/hardState", nodeID.Uint64())),
			offKey:      []byte(fmt.Sprintf("block/%020d/commit", nodeID.Uint64())),
			csKey:       []byte(fmt.Sprintf("block/%020d/confState", nodeID.Uint64())),
			appKey:      []byte(fmt.Sprintf("block/%020d/applied", nodeID.Uint64())),
		},
		snapshotStorage: snapshotStorage{
			snapOp: snapOp,
		},
	}
}

func (l *Log) Delete(ctx context.Context) {
	l.metaStore.Delete(ctx, l.hsKey)
	l.metaStore.Delete(ctx, l.csKey)
	l.metaStore.Delete(ctx, []byte(fmt.Sprintf("block/%020d/compact", l.nodeID.Uint64())))
	l.offsetStore.Delete(l.offKey)
	l.offsetStore.Delete(l.appKey)

	// TODO(james.yin): clean flag in WAL
}
