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
	"github.com/linkall-labs/vanus/internal/store/block/raft"
	"github.com/linkall-labs/vanus/internal/store/block/raw"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/observability/log"
)

// recover recovers state from volume.
func (s *server) recover(ctx context.Context) error {
	ctx, span := s.tracer.Start(ctx, "recover")
	defer span.End()

	metaStore, err := meta.RecoverSyncStore(ctx, s.cfg.MetaStore, filepath.Join(s.volumeDir, "meta"))
	if err != nil {
		return err
	}
	s.metaStore = metaStore

	offsetStore, err := meta.RecoverAsyncStore(ctx, s.cfg.OffsetStore, filepath.Join(s.volumeDir, "offset"))
	if err != nil {
		return err
	}
	s.offsetStore = offsetStore

	// Recover wal and raft logs.
	logs, wal, err := raftlog.RecoverLogsAndWAL(ctx, s.cfg.Raft,
		filepath.Join(s.volumeDir, "raft"), metaStore, offsetStore)
	if err != nil {
		return err
	}
	s.wal = wal

	if err = s.recoverBlocks(ctx, logs); err != nil {
		return err
	}

	return nil
}

func (s *server) recoverBlocks(ctx context.Context, logs map[vanus.ID]*raftlog.Log) error {
	e, _ := raw.ResolveEngine(raw.VSB)

	raws, err := e.Recover(ctx)
	if err != nil {
		return err
	}

	// Recover replicas.
	for id, r := range raws {
		l := logs[id]
		// Raft log has been compacted.
		if l == nil {
			l, err = raftlog.RecoverLog(id, s.wal, s.metaStore, s.offsetStore, nil)
			if err != nil {
				return err
			}
		}
		a := raft.NewAppender(context.TODO(), r, l, s.host, s.leaderChanged)
		s.replicas.Store(id, &replica{
			id:       id,
			idStr:    id.String(),
			engine:   e,
			raw:      r,
			appender: a,
		})
	}

	for id, l := range logs {
		var b *replica
		if v, ok := s.replicas.Load(id); ok {
			b, _ = v.(*replica)
		}

		switch {
		case b == nil:
			log.Debug(ctx, "Not found block, so discard the raft log.", map[string]interface{}{
				"node_id": id,
			})
		default:
			continue
		}

		lastIndex, err2 := l.LastIndex()
		if err2 != nil {
			return err2
		}
		_ = l.Compact(ctx, lastIndex)
	}

	return nil
}
