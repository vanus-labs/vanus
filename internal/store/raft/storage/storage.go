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
	"context"
	"sync"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/executor"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
)

type Storage struct {
	// Protects access to all fields. Most methods of Log are
	// run on the raft goroutine, but Append() and Compact() is run on an
	// application goroutine.
	mu sync.RWMutex

	nodeID vanus.ID

	stateStorage
	logStorage
	snapshotStorage

	// AppendExecutor is the Executor that executes Append, postAppend and Compact.
	AppendExecutor executor.Executor
}

// Make sure Log implements raft.Storage.
var _ raft.Storage = (*Storage)(nil)

// NewStorage creates an empty Storage.
func NewStorage(
	ctx context.Context, nodeID vanus.ID, wal *WAL, stateStore *meta.SyncStore, hintStore *meta.AsyncStore,
	snapOp SnapshotOperator,
) (*Storage, error) {
	if err := wal.addNode(ctx, nodeID); err != nil {
		return nil, err
	}
	return newStorage(nodeID, wal, stateStore, hintStore, snapOp), nil
}

func newStorage(
	nodeID vanus.ID, wal *WAL, stateStore *meta.SyncStore, hintStore *meta.AsyncStore, snapOp SnapshotOperator,
) *Storage {
	s := &Storage{
		nodeID: nodeID,
		logStorage: logStorage{
			// When starting from scratch populate the list with a dummy entry at term zero.
			ents: make([]raftpb.Entry, 1),
			offs: make([]int64, 1),
			wal:  wal,
		},
		stateStorage: stateStorage{
			stateStore: stateStore,
			hintStore:  hintStore,
			hsKey:      []byte(HardStateKey(nodeID.Uint64())),
			offKey:     []byte(CommitKey(nodeID.Uint64())),
			csKey:      []byte(ConfStateKey(nodeID.Uint64())),
			appKey:     []byte(ApplyKey(nodeID.Uint64())),
		},
		snapshotStorage: snapshotStorage{
			snapOp: snapOp,
		},
	}
	return s
}

// Delete discard all data of Storage.
// NOTE: waiting for inflight append calls is the responsibility of the caller.
func (s *Storage) Delete(ctx context.Context) {
	if err := s.wal.removeNode(ctx, s.nodeID); err != nil {
		// TODO(james.yin): handle error.
		panic(err)
	}

	// Clean metadata in stateStore and hintStore.
	s.stateStore.BatchDelete(ctx, [][]byte{s.hsKey, s.csKey}, func(err error) {})
	s.hintStore.BatchDelete([][]byte{s.offKey, s.appKey})
}
