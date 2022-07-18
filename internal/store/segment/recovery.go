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

package segment

import (
	// standard libraries.
	"context"
	"path/filepath"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/store/block/file"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/observability/log"
)

// recover recovers state from volume.
func (s *server) recover(ctx context.Context) error {
	metaStore, err := meta.RecoverSyncStore(s.cfg.MetaStore, filepath.Join(s.volumeDir, "meta"))
	if err != nil {
		return err
	}
	s.metaStore = metaStore

	offsetStore, err := meta.RecoverAsyncStore(s.cfg.OffsetStore, filepath.Join(s.volumeDir, "offset"))
	if err != nil {
		return err
	}
	s.offsetStore = offsetStore

	// Recover wal and raft log.
	raftLogs, wal, err := raftlog.RecoverLogsAndWAL(s.cfg.Raft,
		filepath.Join(s.volumeDir, "raft"), metaStore, offsetStore)
	if err != nil {
		return err
	}
	s.wal = wal

	if err = s.recoverBlocks(ctx, raftLogs); err != nil {
		return err
	}

	return nil
}

func (s *server) recoverBlocks(ctx context.Context, raftLogs map[vanus.ID]*raftlog.Log) error {
	blocks, err := file.Recover(filepath.Join(s.volumeDir, "block"))
	if err != nil {
		return err
	}

	// TODO: optimize this, because the implementation assumes under storage is linux file system
	for blockID, b := range blocks {
		s.blocks.Store(blockID, b)

		// recover replica
		if b.Appendable() {
			raftLog := raftLogs[blockID]
			// Raft log has been compacted.
			if raftLog == nil {
				raftLog, err = raftlog.RecoverLog(blockID, s.wal, s.metaStore, s.offsetStore)
				if err != nil {
					return err
				}
			}
			r := s.makeReplicaWithRaftLog(context.TODO(), b.ID(), b, raftLog)
			b.SetClusterInfoSource(r)
			s.writers.Store(b.ID(), r)
		}
		s.readers.Store(b.ID(), b)
	}

	for nodeID, raftLog := range raftLogs {
		var b *file.Block
		if v, ok := s.blocks.Load(nodeID); ok {
			b, _ = v.(*file.Block)
		}

		switch {
		case b == nil:
			log.Debug(ctx, "Not found block, so discard the raft log.", map[string]interface{}{
				"node_id": nodeID,
			})
		case !b.Appendable():
			log.Debug(ctx, "Block is not appendable, so discard the raft log.", map[string]interface{}{
				"node_id": nodeID,
			})
		default:
			continue
		}

		lastIndex, err2 := raftLog.LastIndex()
		if err2 != nil {
			return err2
		}
		_ = raftLog.Compact(lastIndex)
	}

	return nil
}
