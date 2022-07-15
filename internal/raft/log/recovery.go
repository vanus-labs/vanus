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
	"encoding/binary"
	"fmt"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	storecfg "github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
	"github.com/linkall-labs/vanus/observability/log"
)

func RecoverLogsAndWAL(
	cfg storecfg.RaftConfig, walDir string, metaStore *meta.SyncStore, offsetStore *meta.AsyncStore,
) (map[vanus.ID]*Log, *WAL, error) {
	var compacted int64
	if v, exist := metaStore.Load(walCompactKey); exist {
		var ok bool
		if compacted, ok = v.(int64); !ok {
			panic("raftLog: compacted is not int64")
		}
	}

	logs := make(map[uint64]*Log)
	opts := append([]walog.Option{
		walog.FromPosition(compacted),
		walog.WithRecoveryCallback(func(data []byte, offset int64) error {
			var entry raftpb.Entry
			err := entry.Unmarshal(data)
			if err != nil {
				return err
			}

			l := logs[entry.NodeId]
			if l == nil {
				l = RecoverLog(vanus.NewIDFromUint64(entry.NodeId), nil, metaStore, offsetStore)
				logs[entry.NodeId] = l
			}

			return l.appendInRecovery(entry, offset)
		}),
	}, cfg.WAL.Options()...)
	wal, err := walog.Open(walDir, opts...)
	if err != nil {
		return nil, nil, err
	}
	wal2 := newWAL(wal, metaStore)

	// convert raftLogs, and set wal
	logs2 := make(map[vanus.ID]*Log, len(logs))
	for nodeID, raftLog := range logs {
		raftLog.wal = wal2
		// TODO(james.yin): move to compaction.go
		if offset := raftLog.offs[0]; offset != 0 {
			wal2.barrier.Set(offset, emptyMark)
		}
		logs2[vanus.NewIDFromUint64(nodeID)] = raftLog
	}

	return logs2, wal2, nil
}

func RecoverLog(blockID vanus.ID, wal *WAL, metaStore *meta.SyncStore, offsetStore *meta.AsyncStore) *Log {
	l := NewLog(blockID, wal, metaStore, offsetStore)
	l.recoverCompactionInfo()
	return l
}

func (l *Log) recoverHardState() (raftpb.HardState, error) {
	var hs raftpb.HardState
	if v, exist := l.metaStore.Load(l.hsKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("raftLog: hardState is not []byte")
		}
		if err := hs.Unmarshal(b); err != nil {
			return raftpb.HardState{}, err
		}
	}
	if v, exist := l.offsetStore.Load(l.offKey); exist {
		c, ok := v.(uint64)
		if !ok {
			panic("raftLog: commit is not uint64")
		}
		if c > hs.Commit {
			hs.Commit = c
		}
	}
	return hs, nil
}

func (l *Log) recoverConfState() (raftpb.ConfState, error) {
	var cs raftpb.ConfState
	if v, exist := l.metaStore.Load(l.csKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("raftLog: confState is not []byte")
		}
		if err := cs.Unmarshal(b); err != nil {
			return raftpb.ConfState{}, err
		}
	}
	return cs, nil
}

func (l *Log) recoverApplied() uint64 {
	if v, exist := l.offsetStore.Load(l.appKey); exist {
		app, ok := v.(uint64)
		if !ok {
			panic("raftLog: applied is not uint64")
		}
		return app
	}
	return 0
}

func (l *Log) recoverCompactionInfo() {
	key := fmt.Sprintf("block/%020d/compact", l.nodeID.Uint64())
	if v, ok := l.metaStore.Load([]byte(key)); ok {
		if buf, ok2 := v.([]byte); ok2 {
			dummy := &l.ents[0]
			dummy.Index = binary.BigEndian.Uint64(buf[0:8])
			dummy.Term = binary.BigEndian.Uint64(buf[8:16])
		} else {
			panic("raftLog: compact is not []byte")
		}
	}
}

func (l *Log) appendInRecovery(entry raftpb.Entry, offset int64) error {
	firstInLog := l.firstIndex()
	expectedToAppend := l.lastIndex() + 1
	index := entry.Index

	if expectedToAppend < index {
		log.Error(context.Background(), "missing log entries", map[string]interface{}{
			"lastIndex":     expectedToAppend - 1,
			"appendedIndex": index,
		})
		// FIXME(james.yin): correct error
		return raft.ErrUnavailable
	}

	// Write to cache.
	switch {
	case index < firstInLog:
		// Compacted entry, discard.
	case index == firstInLog:
		// First entry, reset compaction info.
		if offset > l.offs[0] {
			l.ents = []raftpb.Entry{{
				Index: index - 1,
				Term:  entry.Term,
			}, entry}
			if entry.PrevTerm != 0 {
				l.ents[0].Term = entry.PrevTerm
			}
			l.offs = []int64{offset, offset}
		}
	case index == expectedToAppend:
		// Append entry.
		l.ents = append(l.ents, entry)
		l.offs = append(l.offs, offset)
	default:
		// Truncate then append entry.
		si := index - firstInLog + 1
		l.ents = append([]raftpb.Entry{}, l.ents[:si]...)
		l.ents = append(l.ents, entry)
		l.offs = append([]int64{}, l.offs[:si]...)
		l.offs = append(l.offs, offset)
	}

	return nil
}
