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
	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"
)

type snapshotStorage struct{}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (l *Log) Snapshot() (raftpb.Snapshot, error) {
	// TODO(james.yin): snapshot
	// l.RLock()
	// defer l.RUnlock()
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (l *Log) ApplySnapshot(snap raftpb.Snapshot) error {
	l.Lock()
	defer l.Unlock()

	// handle check for old snapshot being applied
	// if l.snapshot.Metadata.Index >= snap.Metadata.Index {
	// 	log.Warning(context.Background(), "snapshot is out of date", map[string]interface{}{})
	// 	return raft.ErrSnapOutOfDate
	// }

	// l.snapshot = snap
	l.ents = []raftpb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}
