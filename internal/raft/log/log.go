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
	"fmt"
	"sync"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
	"github.com/linkall-labs/vanus/observability/log"
)

type Log struct {
	// Protects access to all fields. Most methods of Log are
	// run on the raft goroutine, but Append() and Compact() is run on an
	// application goroutine.
	sync.RWMutex

	nodeID vanus.ID

	// ents[0] is a dummy entry, which record compact information.
	// ents[i] has raft log position i+snapshot.Metadata.Index.
	ents []raftpb.Entry
	// offs[i] is the offset of ents[i] in WAL.
	offs []int64

	wal *WAL

	metaStore   *meta.SyncStore
	offsetStore *meta.AsyncStore

	prevHardSt raftpb.HardState

	hsKey  []byte
	offKey []byte
	csKey  []byte
	appKey []byte
}

// Make sure Log implements raft.Storage.
var _ raft.Storage = (*Log)(nil)

// NewLog creates an empty Log.
func NewLog(nodeID vanus.ID, wal *WAL, metaStore *meta.SyncStore, offsetStore *meta.AsyncStore) *Log {
	return &Log{
		nodeID: nodeID,
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents:        make([]raftpb.Entry, 1),
		offs:        make([]int64, 1),
		wal:         wal,
		metaStore:   metaStore,
		offsetStore: offsetStore,
		hsKey:       []byte(fmt.Sprintf("block/%020d/hardState", nodeID.Uint64())),
		offKey:      []byte(fmt.Sprintf("block/%020d/commit", nodeID.Uint64())),
		csKey:       []byte(fmt.Sprintf("block/%020d/confState", nodeID.Uint64())),
		appKey:      []byte(fmt.Sprintf("block/%020d/applied", nodeID.Uint64())),
	}
}

// InitialState returns the saved HardState and ConfState information.
func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	hs, err := l.recoverHardState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}
	if compacted := l.Compacted(); hs.Commit < compacted {
		hs.Commit = compacted
	}
	l.prevHardSt = hs

	cs, err := l.recoverConfState()
	if err != nil {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	return hs, cs, nil
}

// HardState returns the saved HardState.
//
// NOTE: This method is not thread-safty, it must be used in goroutine which call SetHardState!!!
func (l *Log) HardState() raftpb.HardState {
	return l.prevHardSt
}

// SetHardState saves the current HardState.
func (l *Log) SetHardState(hs raftpb.HardState) (err error) {
	if hs.Term != l.prevHardSt.Term || hs.Vote != l.prevHardSt.Vote {
		var data []byte
		if data, err = hs.Marshal(); err != nil {
			return err
		}
		l.metaStore.Store(l.hsKey, data)
		l.prevHardSt = hs
	} else {
		l.offsetStore.Store(l.offKey, hs.Commit)
		l.prevHardSt.Commit = hs.Commit
	}
	return nil
}

func (l *Log) SetConfState(cs raftpb.ConfState) error {
	data, err := cs.Marshal()
	if err != nil {
		return err
	}
	l.metaStore.Store(l.csKey, data)
	return nil
}

func (l *Log) Applied() uint64 {
	applied := l.recoverApplied()
	if compacted := l.Compacted(); applied < compacted {
		return compacted
	}
	return applied
}

func (l *Log) SetApplied(app uint64) {
	l.offsetStore.Store(l.appKey, app)
}

func (l *Log) Compacted() uint64 {
	return l.ents[0].Index
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (l *Log) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	l.RLock()
	defer l.RUnlock()

	ci := l.compactedIndex()
	if lo <= ci {
		return nil, raft.ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		log.Error(context.Background(), "entries' hi is out of bound lastIndex", map[string]interface{}{
			"hi":        hi,
			"lastIndex": l.lastIndex(),
		})
		return nil, raft.ErrUnavailable
	}
	// no log entry
	if l.length() == 0 {
		return nil, raft.ErrUnavailable
	}

	ents := l.ents[lo-ci : hi-ci]
	return limitSize(ents, maxSize), nil
}

func limitSize(ents []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (l *Log) Term(i uint64) (uint64, error) {
	l.RLock()
	defer l.RUnlock()

	ci := l.compactedIndex()
	if i < ci {
		log.Warning(context.Background(), "raft log has been compacted", map[string]interface{}{
			"index":          i,
			"compactedIndex": ci,
		})
		return 0, raft.ErrCompacted
	}
	if i > l.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	return l.ents[i-ci].Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (l *Log) LastIndex() (uint64, error) {
	l.RLock()
	defer l.RUnlock()
	// FIXME(james.yin): no entry
	return l.lastIndex(), nil
}

func (l *Log) lastIndex() uint64 {
	return l.compactedIndex() + l.length()
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (l *Log) FirstIndex() (uint64, error) {
	l.RLock()
	defer l.RUnlock()
	// FIXME(james.yin): no entry
	return l.firstIndex(), nil
}

func (l *Log) firstIndex() uint64 {
	return l.compactedIndex() + 1
}

func (l *Log) compactedIndex() uint64 {
	return l.ents[0].Index
}

func (l *Log) length() uint64 {
	return uint64(len(l.ents)) - 1
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (l *Log) Snapshot() (raftpb.Snapshot, error) {
	// TODO(james.yin): snapshot
	//l.RLock()
	//defer l.RUnlock()
	return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (l *Log) ApplySnapshot(snap raftpb.Snapshot) error {
	l.Lock()
	defer l.Unlock()

	// handle check for old snapshot being applied
	//if l.snapshot.Metadata.Index >= snap.Metadata.Index {
	//	log.Warning(context.Background(), "snapshot is out of date", map[string]interface{}{})
	//	return raft.ErrSnapOutOfDate
	//}

	//l.snapshot = snap
	l.ents = []raftpb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// Append the new entries to storage.
func (l *Log) Append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	// TODO(james.yin): tolerate discontinuous entries
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			log.Warning(context.Background(), "entries to append are discontinuous", map[string]interface{}{})
			// FIXME(james.yin): error
			return raft.ErrUnavailable
		}
		if entries[i].Term != entries[i-1].Term {
			entries[i].PrevTerm = entries[i-1].PrevTerm
		}
	}

	l.Lock()
	defer l.Unlock()

	firstInLog := l.firstIndex()
	expectedToAppend := l.lastIndex() + 1
	firstToAppend := entries[0].Index

	if expectedToAppend < firstToAppend {
		log.Error(context.Background(), "missing log entries", map[string]interface{}{
			"lastIndex":     expectedToAppend - 1,
			"appendedIndex": firstToAppend,
		})
		// FIXME(james.yin): correct error
		return raft.ErrUnavailable
	}

	lastToAppend := firstToAppend + uint64(len(entries)) - 1 // entries[len(entries)-1].Index

	// Shortcut if there is no new entry.
	if lastToAppend < firstInLog {
		return nil
	}

	// Truncate compacted entries.
	if firstToAppend < firstInLog {
		entries = entries[firstInLog-firstToAppend:]
		firstToAppend = entries[0].Index
	}

	pi := firstToAppend - firstInLog
	if entries[0].Term != l.ents[pi].Term {
		entries[0].PrevTerm = l.ents[pi].Term
	}

	// Append to WAL.
	var err error
	var offsets []int64
	func() {
		l.Unlock()
		defer l.Lock()

		if firstToAppend == firstInLog {
			if l.wal.suppressCompact(func() (compactTask, error) {
				if offsets, err = l.appendToWAL(entries); err != nil {
					return compactTask{}, err
				}
				return compactTask{
					offset: offsets[0],
					last:   l.offs[0],
				}, nil
			}) == nil {
				// Record offset of first entry in WAL.
				l.offs[0] = offsets[0]
			}
		} else {
			offsets, err = l.appendToWAL(entries)
		}
	}()
	if err != nil {
		// FIXME(james.yin): correct error
		return err
	}

	// Write to cache, and record offset in WAL.
	if firstToAppend == expectedToAppend {
		// append
		l.ents = append(l.ents, entries...)
		l.offs = append(l.offs, offsets...)
	} else {
		// truncate then append: firstToAppend < expectedToAppend
		si := pi + 1
		l.ents = append([]raftpb.Entry{}, l.ents[:si]...)
		l.ents = append(l.ents, entries...)
		l.offs = append([]int64{}, l.offs[:si]...)
		l.offs = append(l.offs, offsets...)
	}

	return nil
}

func (l *Log) appendToWAL(entries []raftpb.Entry) ([]int64, error) {
	ents := make([][]byte, len(entries))
	for i, entry := range entries {
		// reset node ID.
		entry.NodeId = l.nodeID.Uint64()
		ent, err := entry.Marshal()
		if err != nil {
			return nil, err
		}
		ents[i] = ent
	}
	return l.wal.Append(ents, walog.WithoutBatching()).Wait()
}
