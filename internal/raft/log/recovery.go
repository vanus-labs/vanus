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
	"strings"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	storecfg "github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
	"github.com/linkall-labs/vanus/observability/log"
)

func RecoverLogsAndWAL(
	ctx context.Context,
	cfg storecfg.RaftConfig,
	walDir string,
	metaStore *meta.SyncStore,
	offsetStore *meta.AsyncStore,
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
		walog.WithRecoveryCallback(func(data []byte, r walog.Range) error {
			var entry raftpb.Entry
			err := entry.Unmarshal(data)
			if err != nil {
				return err
			}

			l := logs[entry.NodeId]
			if l == nil {
				l, err = RecoverLog(vanus.NewIDFromUint64(entry.NodeId), nil, metaStore, offsetStore, nil)
				if err != nil {
					return err
				}
				logs[entry.NodeId] = l
			}

			return l.appendInRecovery(ctx, entry, r.SO)
		}),
	}, cfg.WAL.Options()...)
	wal, err := walog.Open(ctx, walDir, opts...)
	if err != nil {
		return nil, nil, err
	}
	wal2 := newWAL(wal, metaStore) //nolint:contextcheck // wrong advice

	// convert raftLogs, and set wal
	logs2 := make(map[vanus.ID]*Log, len(logs))
	for nodeID, raftLog := range logs {
		raftLog.wal = wal2
		// TODO(james.yin): move to compaction.go
		if raftLog.length() != 0 {
			off := raftLog.offs[1]
			raftLog.offs[0] = off
			wal2.barrier.Set(off, emptyMark)
		}
		logs2[vanus.NewIDFromUint64(nodeID)] = raftLog
	}

	return logs2, wal2, nil
}

func RecoverLog(
	blockID vanus.ID, wal *WAL, metaStore *meta.SyncStore, offsetStore *meta.AsyncStore, snapOp SnapshotOperator,
) (*Log, error) {
	l := NewLog(blockID, wal, metaStore, offsetStore, snapOp)

	if err := l.recoverCompactionInfo(); err != nil {
		return nil, err
	}

	if err := l.recoverState(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) recoverState() error {
	hs, err := l.recoverHardState()
	if err != nil {
		return err
	}
	if compacted := l.Compacted(); hs.Commit < compacted {
		hs.Commit = compacted
	}
	l.prevHardSt = hs

	cs, err := l.recoverConfState()
	if err != nil {
		return err
	}
	l.prevConfSt = cs

	app, err := l.recoverApplied()
	if err != nil {
		return err
	}
	if com := l.Compacted(); app < com {
		app = com
	}
	l.prevApply = app

	return nil
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

func (l *Log) recoverApplied() (uint64, error) {
	if v, exist := l.offsetStore.Load(l.appKey); exist {
		app, ok := v.(uint64)
		if !ok {
			panic("raftLog: applied is not uint64")
		}
		return app, nil
	}
	return 0, nil
}

func (l *Log) recoverCompactionInfo() error {
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
	return nil
}

func (l *Log) appendInRecovery(_ context.Context, entry raftpb.Entry, so int64) error {
	term := entry.Term
	index := entry.Index

	compactedTerm := l.compactedTerm()
	firstIndex := l.firstIndex()
	lastTerm := l.lastTerm()
	lastIndex := l.lastIndex()
	expectedIndex := lastIndex + 1

	var reason string

	// Compacted entry, discard.
	if term < compactedTerm {
		return nil
	}
	if index < firstIndex {
		// All compacted entries are committed, and committed entries are immutable.
		if term != compactedTerm || l.length() != 0 {
			reason = "roll back to the index before compacted"
			goto PANIC
		}
		return nil
	}

	// Write to cache.
	switch {
	case term < lastTerm:
		// Term will not roll back.
		reason = "term roll back"
		goto PANIC
	case index > expectedIndex:
		reason = "missing log entries"
		goto PANIC
	case index == expectedIndex:
		// Append entry.
		l.ents = append(l.ents, entry)
		l.offs = append(l.offs, so)
	case term > lastTerm:
		// Truncate, then append entry.
		si := index - firstIndex + 1
		l.ents = append([]raftpb.Entry{}, l.ents[:si]...)
		l.ents = append(l.ents, entry)
		l.offs = append([]int64{}, l.offs[:si]...)
		l.offs = append(l.offs, so)
	default:
		// In the same term, index is monotonically increasing.
		reason = "index roll back in term"
		goto PANIC
	}
	return nil

PANIC:
	log.Error(context.Background(), fmt.Sprintf("%s%s.", strings.ToUpper(reason[0:1]), reason[1:]), map[string]interface{}{
		"node_id":    l.nodeID,
		"last_term":  lastTerm,
		"last_index": lastIndex,
		"next_term":  term,
		"next_index": index,
	})
	panic(fmt.Sprintf("unreachable: %s", reason))
}
