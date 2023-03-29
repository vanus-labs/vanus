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
	"context"
	"errors"

	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/raft"
	"github.com/vanus-labs/vanus/raft/raftpb"

	// this project.
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

var (
	ErrNoEntry   = errors.New("no entry")
	ErrBadEntry  = errors.New("bad entry")
	ErrCompacted = errors.New("appending entries has been compacted")
	ErrTruncated = errors.New("appending entries has been truncated")
)

type logStorage struct {
	// ents[0] is a dummy entry, which record compact information.
	// ents[i] has raft log position i+snapshot.Metadata.Index.
	ents []raftpb.Entry
	// offs[0] is a dummy entry, which records last offset where the barrier was set.
	// offs[i] is the start offset of ents[i] in WAL.
	offs []int64
	tail int64

	wal *WAL
}

func (s *Storage) Compacted() uint64 {
	return s.ents[0].Index
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ci := s.compactedIndex()
	if lo <= ci {
		return nil, raft.ErrCompacted
	}
	if hi > s.lastIndex()+1 {
		log.Error().
			Uint64("hi", hi).
			Uint64("last_index", s.lastIndex()).
			Msg("entries' hi is out of bound lastIndex")
		return nil, raft.ErrUnavailable
	}
	// no log entry
	if s.length() == 0 {
		return nil, raft.ErrUnavailable
	}

	ents := s.ents[lo-ci : hi-ci]
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
func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.term(i)
}

func (s *Storage) term(i uint64) (uint64, error) {
	ci := s.compactedIndex()
	if i < ci {
		log.Warn().
			Uint64("index", i).
			Uint64("compactedIndex", ci).
			Msg("raft log has been compacted")
		return 0, raft.ErrCompacted
	}
	if i > s.lastIndex() {
		return 0, raft.ErrUnavailable
	}
	return s.ents[i-ci].Term, nil
}

func (s *Storage) lastTerm() uint64 {
	return s.ents[s.length()].Term
}

func (s *Storage) lastStableTerm() uint64 { //nolint:unused // ok
	return s.ents[s.stableLength()].Term
}

func (s *Storage) compactedTerm() uint64 {
	return s.ents[0].Term
}

// LastIndex returns the index of the last entry in the log.
func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// FIXME(james.yin): no entry
	return s.lastIndex(), nil
}

func (s *Storage) lastIndex() uint64 {
	return s.compactedIndex() + s.length()
}

func (s *Storage) lastStableIndex() uint64 {
	return s.compactedIndex() + s.stableLength()
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// FIXME(james.yin): no entry
	return s.firstIndex(), nil
}

func (s *Storage) firstIndex() uint64 {
	return s.compactedIndex() + 1
}

func (s *Storage) compactedIndex() uint64 {
	return s.ents[0].Index
}

func (s *Storage) length() uint64 {
	return uint64(len(s.ents)) - 1
}

func (s *Storage) stableLength() uint64 {
	return uint64(len(s.offs)) - 1
}

type AppendResult struct {
	Term  uint64
	Index uint64
}

type AppendCallback = func(AppendResult, error)

// Append appends the new entries to storage.
// After the call returns, all entries are readable. After the AppendCallback cb fires, all entries are persisted.
// NOTE: Synchronization is the responsibility of the caller.
func (s *Storage) Append(ctx context.Context, entries []raftpb.Entry, cb AppendCallback) { //nolint:funlen // ok
	if len(entries) == 0 {
		cb(AppendResult{}, ErrNoEntry)
		return
	}

	if err := s.prepareAppend(ctx, entries); err != nil {
		cb(AppendResult{}, err)
		return
	}

	term := entries[0].Term
	index := entries[0].Index
	rindex := index + uint64(len(entries)) - 1 // entries[len(entries)-1].Index

	firstIndex := s.firstIndex()
	lastTerm := s.lastTerm()
	lastIndex := s.lastIndex()
	expectedIndex := lastIndex + 1

	if expectedIndex < index {
		log.Error(ctx).
			Stringer("node_id", s.nodeID).
			Uint64("first_index", firstIndex).
			Uint64("last_term", lastTerm).
			Uint64("last_index", lastIndex).
			Uint64("next_term", term).
			Uint64("next_index", index).
			Msg("Missing log entries.")
		cb(AppendResult{}, ErrBadEntry)
		return
	}

	// Shortcut if there is no new entry.
	if rindex < firstIndex {
		cb(AppendResult{}, ErrCompacted)
		return
	}

	// Truncate compacted entries.
	if index < firstIndex {
		entries = entries[firstIndex-index:]
		term = entries[0].Term
		index = entries[0].Index
	}

	if term < lastTerm {
		log.Error(ctx).
			Stringer("node_id", s.nodeID).
			Uint64("first_index", firstIndex).
			Uint64("last_term", lastTerm).
			Uint64("last_index", lastIndex).
			Uint64("next_term", term).
			Uint64("next_index", index).
			Msg("Term roll back.")
		cb(AppendResult{}, ErrBadEntry)
		return
	}

	if prev := &s.ents[index-firstIndex]; term != prev.Term {
		entries[0].PrevTerm = prev.Term
	}

	bytes, err := s.marshalEntries(entries)
	if err != nil {
		cb(AppendResult{}, err)
		return
	}

	var ents []raftpb.Entry
	if index == expectedIndex {
		// append
		ents = append(s.ents, entries...) //nolint:gocritic // assign below
	} else {
		// truncate then append: term > lastTerm
		i := index - firstIndex + 1
		ents = append([]raftpb.Entry{}, s.ents[:i]...)
		ents = append(ents, entries...)
		// truncate offsets
		if index < s.lastStableIndex() {
			s.offs = append([]int64{}, s.offs[:i]...)
		}
	}

	s.mu.Lock()
	s.ents = ents
	s.mu.Unlock()

	remark := index == firstIndex

	// Append to WAL.
	s.wal.Append(ctx, bytes, func(ranges []walog.Range, err error) {
		if err != nil {
			panic(err)
		}

		if remark {
			// Mark barrier on the offset of first entry in WAL.
			_ = s.wal.markBarrier(context.TODO(), s.nodeID, ranges[0].SO)
		}

		offsets := make([]int64, len(ranges))
		for i, r := range ranges {
			offsets[i] = r.SO
		}
		tail := ranges[len(ranges)-1].EO
		_ = s.AppendExecutor.Execute(func() {
			s.postAppend(entries, offsets, tail, remark, cb)
		})
		// FIXME(james.yin): appender is deleted.
	})
}

func (s *Storage) postAppend(entries []raftpb.Entry, offsets []int64, tail int64, remark bool, cb AppendCallback) {
	end := len(entries) - 1
	for ; end >= 0; end-- {
		e := &entries[end]
		gt, err := s.term(e.Index)
		if err == nil && gt == e.Term {
			break
		}
	}

	if end < 0 {
		if remark {
			// Remove obsolete barrier from truncated entry.
			_ = s.wal.removeBarrier(context.TODO(), s.nodeID, offsets[0])
		}
		// All entries has been truncated.
		cb(AppendResult{}, ErrTruncated)
		return
	}

	index := entries[0].Index
	fi := s.firstIndex()
	li := s.lastStableIndex()

	if index == li+1 {
		// append
		s.offs = append(s.offs, offsets[:end+1]...)
	} else {
		// truncate then append: term > lastTerm
		after := index - fi + 1
		s.offs = append([]int64{}, s.offs[:after]...)
		s.offs = append(s.offs, offsets[:end+1]...)
	}
	s.tail = tail

	if remark {
		// Remove obsolete barrier.
		if s.offs[0] != 0 {
			_ = s.wal.removeBarrier(context.TODO(), s.nodeID, s.offs[0])
		}

		// Record barrier.
		s.offs[0] = s.offs[1]
	}

	e := &entries[end]
	cb(AppendResult{
		Term:  e.Term,
		Index: e.Index,
	}, nil)
}

func (s *Storage) prepareAppend(ctx context.Context, entries []raftpb.Entry) error {
	for i := 1; i < len(entries); i++ {
		entry, prev := &entries[i], &entries[i-1]
		var reason string
		if entry.Index != prev.Index+1 {
			reason = "Entries to append are discontinuous."
		} else if entry.Term < prev.Term {
			reason = "Term roll back."
		}
		if reason != "" {
			log.Warn(ctx).
				Stringer("node_id", s.nodeID).
				Uint64("term", entry.Term).
				Uint64("index", entry.Index).
				Uint64("previous_term", prev.Term).
				Uint64("previous_index", prev.Index).
				Msg(reason)
			return ErrBadEntry
		}
		if entry.Term != prev.Term {
			entry.PrevTerm = prev.Term
		}
	}
	return nil
}

func (s *Storage) marshalEntries(entries []raftpb.Entry) ([][]byte, error) {
	ents := make([][]byte, len(entries))
	for i, entry := range entries {
		// reset node ID.
		entry.NodeId = s.nodeID.Uint64()
		ent, err := entry.Marshal()
		if err != nil {
			return nil, err
		}
		ents[i] = ent
	}
	return ents, nil
}
