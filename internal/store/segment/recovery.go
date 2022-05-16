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
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/internal/store/segment/block"
	"github.com/linkall-labs/vanus/observability/log"
)

func (s *segmentServer) recover(ctx context.Context) error {
	metaStore, err := meta.RecoverSyncStore(filepath.Join(s.volumeDir, "meta"))
	if err != nil {
		return err
	}
	s.metaStore = metaStore

	offsetStore, err := meta.RecoverAsyncStore(filepath.Join(s.volumeDir, "offset"))
	if err != nil {
		return err
	}
	s.offsetStore = offsetStore

	// Recover wal and raft log.
	raftLogs, wal, err := raftlog.RecoverLogsAndWAL(filepath.Join(s.volumeDir, "raft"), metaStore, offsetStore)
	if err != nil {
		return err
	}
	s.wal = wal

	if err = s.recoverBlocks(ctx, raftLogs); err != nil {
		return err
	}

	return nil
}

func (s *segmentServer) recoverBlocks(ctx context.Context, raftLogs map[vanus.ID]*raftlog.Log) error {
	blocks, err := block.RecoverBlocks(filepath.Join(s.volumeDir, "block"))
	if err != nil {
		return err
	}

	// TODO: optimize this, because the implementation assumes under storage is linux file system
	for blockID, path := range blocks {
		b, err := block.OpenFileSegmentBlock(ctx, path)
		if err != nil {
			return err
		}

		log.Info(ctx, "The block was loaded.", map[string]interface{}{
			"blockID": blockID,
		})
		// TODO(james.yin): initialize block
		if err = b.Initialize(ctx); err != nil {
			return err
		}

		s.blocks.Store(blockID, b)

		// recover replica
		if b.IsAppendable() {
			raftLog := raftLogs[blockID]
			// raft log has been compacted
			if raftLog == nil {
				raftLog = raftlog.RecoverLog(blockID, s.wal, s.metaStore, s.offsetStore)
			}
			replica := s.makeReplicaWithRaftLog(context.TODO(), b, raftLog)
			s.blockWriters.Store(b.SegmentBlockID(), replica)
		}
		s.blockReaders.Store(b.SegmentBlockID(), b)
	}

	for nodeID, raftLog := range raftLogs {
		b, ok := s.blocks.Load(nodeID)
		if !ok {
			// TODO(james.yin): no block for raft log, compact
			log.Debug(ctx, "Not found block, so discard the raft log.", map[string]interface{}{
				"nodeID": nodeID,
			})
			continue
		}
		if _, ok = b.(*block.Replica); !ok {
			// TODO(james.yin): block is not appendable, compact
			log.Debug(ctx, "Block is not appendable, so discard the raft log.", map[string]interface{}{
				"nodeID": nodeID,
			})
			continue
		}
		_ = raftLog
	}

	return nil
}
