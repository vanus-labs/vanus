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

//go:generate mockgen -source=snapshot_storage.go -destination=mock_snapshot_storage.go -package=storage
package storage

import (
	// standard libraries.
	"context"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/pkg/observability/log"
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
func (s *Storage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.snapOp == nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	term, err := s.term(s.prevApply)
	if err != nil {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}

	data, err := s.snapOp.GetSnapshot(s.prevApply)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			ConfState: s.prevConfSt,
			Index:     s.prevApply,
			Term:      term,
		},
		Data: data,
	}
	return snap, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (s *Storage) ApplySnapshot(ctx context.Context, snap raftpb.Snapshot) error {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("raft.log.Log.ApplySnapshot() Start")
	defer span.AddEvent("raft.log.Log.ApplySnapshot() End")

	s.mu.Lock()
	defer s.mu.Unlock()

	// Handle check for old snapshot being applied.
	if s.lastIndex() >= snap.Metadata.Index {
		log.Warn().Msg("snapshot is out of date")
		return nil
	}

	if err := s.snapOp.ApplySnapshot(snap.Data); err != nil {
		return err
	}

	last := s.offs[0]
	s.ents = []raftpb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	s.offs = []int64{0}
	_ = s.wal.tryCompact(ctx, s.nodeID, 0, last, 0, snap.Metadata.Index, snap.Metadata.Term)
	s.SetApplied(ctx, snap.Metadata.Index)

	return nil
}
