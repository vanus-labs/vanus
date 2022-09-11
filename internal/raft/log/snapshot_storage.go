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

//go:generate mockgen -source=snapshot_storage.go -destination=mock_snapshot_storage.go -package=log
package log

import (
	// standard libraries.
	"context"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/observability/log"
)

type SnapshotOperator interface {
	GetSnapshot(index uint64) ([]byte, error)
	ApplySnapshot(data []byte) error
}

type snapshotStorage struct {
	snapOp SnapshotOperator
}

func (ss *snapshotStorage) SetSnapshotOperator(op SnapshotOperator) {
	ss.snapOp = op
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (l *Log) Snapshot() (raftpb.Snapshot, error) {
	l.RLock()
	defer l.RUnlock()

	if l.snapOp == nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	term, err := l.term(l.prevApply)
	if err != nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	data, err := l.snapOp.GetSnapshot(l.prevApply)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	s := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: l.prevConfSt,
			Index:     l.prevApply,
			Term:      term,
		},
		Data: data,
	}
	return s, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (l *Log) ApplySnapshot(ctx context.Context, snap raftpb.Snapshot) error {
	l.Lock()
	defer l.Unlock()

	// Handle check for old snapshot being applied.
	if l.lastIndex() >= snap.Metadata.Index {
		log.Warning(context.Background(), "snapshot is out of date", map[string]interface{}{})
		return nil
	}

	if err := l.snapOp.ApplySnapshot(snap.Data); err != nil {
		return err
	}

	last := l.offs[0]
	l.ents = []raftpb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	l.offs = []int64{0}
	l.wal.tryCompact(ctx, 0, last, l.nodeID, snap.Metadata.Index, snap.Metadata.Term)
	l.SetApplied(snap.Metadata.Index)

	return nil
}
