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

	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/store/meta"
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

type storagesBuilder struct {
	ctx        context.Context
	stateStore *meta.SyncStore
	hintStore  *meta.AsyncStore
	storages   map[uint64]*Storage
}

var compactSuffix = []byte("/compact")

const (
	nodeIDStart      = 6 // block/
	compactSuffixLen = 8 // /compact
	minCompactKeyLen = nodeIDStart + compactSuffixLen + 1
)

func (sb *storagesBuilder) onMeta(key []byte, value interface{}) error {
	if len(key) < minCompactKeyLen || !bytes.Equal(key[len(key)-compactSuffixLen:], compactSuffix) {
		return nil
	}

	id, err := strconv.ParseUint(string(key[nodeIDStart:len(key)-compactSuffixLen]), 10, 64)
	if err != nil {
		log.Warning(sb.ctx, "raftStorage: invalid compact key", map[string]interface{}{
			"key": string(key),
		})
		return nil //nolint:nilerr // skip
	}

	info, ok := value.([]byte)
	if !ok {
		panic("raftStorage: compact is not []byte") // unreachable
	}

	s, err := recoverStorage(vanus.NewIDFromUint64(id), nil, sb.stateStore, sb.hintStore, info, nil)
	if err != nil {
		return err
	}
	sb.storages[id] = s

	return nil
}

func (sb *storagesBuilder) onEntry(data []byte, r walog.Range) error {
	var entry raftpb.Entry
	err := entry.Unmarshal(data)
	if err != nil {
		return err
	}

	s := sb.storages[entry.NodeId]
	if s == nil {
		return nil
	}

	return s.appendInRecovery(sb.ctx, entry, r.SO)
}

var (
	rangeStartKey = []byte("block/\000")
	rangeEndKey   = []byte("block0")
)

func Recover(
	ctx context.Context, dir string, stateStore *meta.SyncStore, hintStore *meta.AsyncStore, opts ...walog.Option,
) (map[vanus.ID]*Storage, *WAL, error) {
	sb := &storagesBuilder{
		ctx:        ctx,
		stateStore: stateStore,
		hintStore:  hintStore,
		storages:   make(map[uint64]*Storage),
	}
	if err := stateStore.Range(rangeStartKey, rangeEndKey, sb.onMeta); err != nil {
		return nil, nil, err
	}

	var compacted int64
	if v, exist := stateStore.Load(walCompactKey); exist {
		var ok bool
		if compacted, ok = v.(int64); !ok {
			panic("raftStorage: compacted is not int64")
		}
	}

	opts = append([]walog.Option{
		walog.FromPosition(compacted),
		walog.WithRecoveryCallback(sb.onEntry),
	}, opts...)
	wal, err := walog.Open(ctx, dir, opts...)
	if err != nil {
		return nil, nil, err
	}
	wal2 := newWAL(wal, stateStore)

	// convert, and set wal
	storages := make(map[vanus.ID]*Storage, len(sb.storages))
	for id, storage := range sb.storages {
		nodeID := vanus.NewIDFromUint64(id)
		storage.wal = wal2
		// TODO(james.yin): move to compaction.go
		var off int64
		if storage.length() != 0 {
			off = storage.offs[1]
			storage.offs[0] = off
		}
		wal2.recoverNode(nodeID, off)
		storages[nodeID] = storage
	}

	return storages, wal2, nil
}

func RecoverStorage(
	nodeID vanus.ID, wal *WAL, stateStore *meta.SyncStore, hintStore *meta.AsyncStore, snapOp SnapshotOperator,
) (*Storage, error) {
	return recoverStorage(nodeID, wal, stateStore, hintStore, nil, snapOp)
}

func recoverStorage(
	nodeID vanus.ID, wal *WAL, stateStore *meta.SyncStore, hintStore *meta.AsyncStore, info []byte,
	snapOp SnapshotOperator,
) (*Storage, error) {
	s := newStorage(nodeID, wal, stateStore, hintStore, snapOp)

	if err := s.recoverCompactionInfo(info); err != nil {
		return nil, err
	}

	if err := s.recoverState(); err != nil {
		return nil, err
	}

	if wal != nil {
		wal.recoverNode(nodeID, 0)
	}

	return s, nil
}

func (s *Storage) recoverState() error {
	hs, err := s.recoverHardState()
	if err != nil {
		return err
	}
	if compacted := s.Compacted(); hs.Commit < compacted {
		hs.Commit = compacted
	}
	s.prevHardSt = hs

	cs, err := s.recoverConfState()
	if err != nil {
		return err
	}
	s.prevConfSt = cs

	app, err := s.recoverApplied()
	if err != nil {
		return err
	}
	if com := s.Compacted(); app < com {
		app = com
	}
	s.prevApply = app

	return nil
}

func (s *Storage) recoverHardState() (raftpb.HardState, error) {
	var hs raftpb.HardState
	if v, exist := s.stateStore.Load(s.hsKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("raftStorage: hardState is not []byte")
		}
		if err := hs.Unmarshal(b); err != nil {
			return raftpb.HardState{}, err
		}
	}
	if v, exist := s.hintStore.Load(s.offKey); exist {
		c, ok := v.(uint64)
		if !ok {
			panic("raftStorage: commit is not uint64")
		}
		if c > hs.Commit {
			hs.Commit = c
		}
	}
	return hs, nil
}

func (s *Storage) recoverConfState() (raftpb.ConfState, error) {
	var cs raftpb.ConfState
	if v, exist := s.stateStore.Load(s.csKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("raftStorage: confState is not []byte")
		}
		if err := cs.Unmarshal(b); err != nil {
			return raftpb.ConfState{}, err
		}
	}
	return cs, nil
}

func (s *Storage) recoverApplied() (uint64, error) {
	if v, exist := s.hintStore.Load(s.appKey); exist {
		app, ok := v.(uint64)
		if !ok {
			panic("raftStorage: applied is not uint64")
		}
		return app, nil
	}
	return 0, nil
}

func (s *Storage) recoverCompactionInfo(info []byte) error {
	if info == nil {
		key := []byte(CompactKey(s.nodeID.Uint64()))
		v, ok := s.stateStore.Load(key)
		if !ok {
			return nil
		}

		info, ok = v.([]byte)
		if !ok {
			panic("raftStorage: compact is not []byte") // unreachable
		}
	}

	dummy := &s.ents[0]
	dummy.Index = binary.BigEndian.Uint64(info[0:8])
	dummy.Term = binary.BigEndian.Uint64(info[8:16])
	return nil
}

func (s *Storage) appendInRecovery(_ context.Context, entry raftpb.Entry, so int64) error {
	term := entry.Term
	index := entry.Index

	compactedTerm := s.compactedTerm()
	firstIndex := s.firstIndex()
	lastTerm := s.lastTerm()
	lastIndex := s.lastIndex()
	expectedIndex := lastIndex + 1

	// Compacted entry, discard.
	if term < compactedTerm {
		return nil
	}
	if index < firstIndex {
		// All compacted entries are committed, and committed entries are immutable.
		if term != compactedTerm || s.length() != 0 {
			unreachablePanic(
				"roll back to the index before compacted", s.nodeID, lastTerm, lastIndex, term, index)
		}
		return nil
	}

	// Write to cache.
	switch {
	case term < lastTerm:
		// Term will not roll back.
		unreachablePanic("term roll back", s.nodeID, lastTerm, lastIndex, term, index)
	case index > expectedIndex:
		unreachablePanic("missing log entries", s.nodeID, lastTerm, lastIndex, term, index)
	case index == expectedIndex:
		// Append entry.
		s.ents = append(s.ents, entry)
		s.offs = append(s.offs, so)
	case term > lastTerm:
		// Truncate, then append entry.
		si := index - firstIndex + 1
		s.ents = append([]raftpb.Entry{}, s.ents[:si]...)
		s.ents = append(s.ents, entry)
		s.offs = append([]int64{}, s.offs[:si]...)
		s.offs = append(s.offs, so)
	default:
		// In the same term, index is monotonically increasing.
		unreachablePanic("index roll back in term", s.nodeID, lastTerm, lastIndex, term, index)
	}
	return nil
}

func unreachablePanic(reason string, nodeID vanus.ID, lastTerm, lastIndex, term, index uint64) {
	log.Error(
		context.Background(),
		fmt.Sprintf("%s%s.", strings.ToUpper(reason[0:1]), reason[1:]),
		map[string]interface{}{
			"node_id":    nodeID,
			"last_term":  lastTerm,
			"last_index": lastIndex,
			"next_term":  term,
			"next_index": index,
		})
	panic(fmt.Sprintf("unreachable: %s", reason))
}
