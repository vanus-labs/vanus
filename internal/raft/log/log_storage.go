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
	"errors"

	// third-party libraries.
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	walog "github.com/linkall-labs/vanus/internal/store/wal"
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

	appendResultC chan appendTask

	wal *WAL
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
			"hi":         hi,
			"last_index": l.lastIndex(),
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
	return l.term(i)
}

func (l *Log) term(i uint64) (uint64, error) {
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

func (l *Log) lastTerm() uint64 {
	return l.ents[l.length()].Term
}

func (l *Log) lastStableTerm() uint64 { //nolint:unused // ok
	return l.ents[l.stableLength()].Term
}

func (l *Log) compactedTerm() uint64 {
	return l.ents[0].Term
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

func (l *Log) lastStableIndex() uint64 {
	return l.compactedIndex() + l.stableLength()
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

func (l *Log) stableLength() uint64 {
	return uint64(len(l.offs)) - 1
}

type AppendResult struct {
	Term  uint64
	Index uint64
}

type appendTask struct {
	entries []raftpb.Entry
	offsets []int64
	err     error
	cb      func(AppendResult, error)
}

type AppendCallback = func(AppendResult, error)

// Append the new entries to storage.
// After the call returns, all entries are readable. After the AppendCallback cb fires, all entries are persisted.
func (l *Log) Append(ctx context.Context, entries []raftpb.Entry, cb AppendCallback) { //nolint:funlen // ok
	span := trace.SpanFromContext(ctx)
	span.AddEvent("raft.log.Log.Append() Start")
	defer span.AddEvent("raft.log.Log.Append() End")

	if len(entries) == 0 {
		t := appendTask{
			err: ErrNoEntry,
			cb:  cb,
		}
		l.appendResultC <- t
		return
	}

	if err := l.prepareAppend(ctx, entries); err != nil {
		t := appendTask{
			err: err,
			cb:  cb,
		}
		l.appendResultC <- t
		return
	}

	span.AddEvent("Acquiring lock")
	l.Lock()
	span.AddEvent("Got lock")

	term := entries[0].Term
	index := entries[0].Index
	rindex := index + uint64(len(entries)) - 1 // entries[len(entries)-1].Index

	firstIndex := l.firstIndex()
	lastTerm := l.lastTerm()
	lastIndex := l.lastIndex()
	expectedIndex := lastIndex + 1

	if expectedIndex < index {
		l.Unlock()
		log.Error(ctx, "Missing log entries.", map[string]interface{}{
			"node_id":     l.nodeID,
			"first_index": firstIndex,
			"last_term":   lastTerm,
			"last_index":  lastIndex,
			"next_term":   term,
			"next_index":  index,
		})
		t := appendTask{
			err: ErrBadEntry,
			cb:  cb,
		}
		l.appendResultC <- t
		return
	}

	// Shortcut if there is no new entry.
	if rindex < firstIndex {
		l.Unlock()
		t := appendTask{
			err: ErrCompacted,
			cb:  cb,
		}
		l.appendResultC <- t
		return
	}

	// Truncate compacted entries.
	if index < firstIndex {
		entries = entries[firstIndex-index:]
		term = entries[0].Term
		index = entries[0].Index
	}

	if term < lastTerm {
		l.Unlock()
		log.Error(ctx, "Term roll back.", map[string]interface{}{
			"node_id":     l.nodeID,
			"first_index": firstIndex,
			"last_term":   lastTerm,
			"last_index":  lastIndex,
			"next_term":   term,
			"next_index":  index,
		})
		t := appendTask{
			err: ErrBadEntry,
			cb:  cb,
		}
		l.appendResultC <- t
		return
	}

	if prev := &l.ents[index-firstIndex]; term != prev.Term {
		entries[0].PrevTerm = prev.Term
	}

	if index == expectedIndex {
		// append
		l.ents = append(l.ents, entries...)
	} else {
		// truncate then append: term > lastTerm
		i := index - firstIndex + 1
		l.ents = append([]raftpb.Entry{}, l.ents[:i]...)
		l.ents = append(l.ents, entries...)
		// truncate offsets
		if index < l.lastStableIndex() {
			l.offs = append([]int64{}, l.offs[:i]...)
		}
	}

	span.AddEvent("Releasing lock")
	l.Unlock()

	ents, err := l.marshalEntries(entries)
	if err != nil {
		l.appendResultC <- appendTask{
			err: err,
			cb:  cb,
		}
		return
	}

	// Append to WAL.
	l.appendToWAL(ctx, ents, index == firstIndex, func(ranges []walog.Range, err error) {
		if err != nil {
			l.appendResultC <- appendTask{
				err: err,
				cb:  cb,
			}
			return
		}

		offsets := make([]int64, len(ranges))
		for i, r := range ranges {
			offsets[i] = r.SO
		}
		l.appendResultC <- appendTask{
			entries: entries,
			offsets: offsets,
			cb:      cb,
		}
	})
}

func (l *Log) runPostAppend() {
	for task := range l.appendResultC {
		l.doPostAppend(task)
	}
}

func (l *Log) doPostAppend(task appendTask) {
	if task.err != nil {
		task.cb(AppendResult{}, task.err)
		return
	}

	l.Lock()

	end := len(task.entries) - 1
	for ; end >= 0; end-- {
		e := task.entries[end]
		gt, err := l.term(e.Index)
		if err == nil && gt == e.Term {
			break
		}
	}

	if end < 0 {
		// All entries has been truncated.
		l.Unlock()
		task.cb(AppendResult{}, ErrTruncated)
		return
	}

	index := task.entries[0].Index
	fi := l.firstIndex()
	li := l.lastStableIndex()

	if index == li+1 {
		// append
		l.offs = append(l.offs, task.offsets[:end+1]...)
	} else {
		// truncate then append: term > lastTerm
		after := index - fi + 1
		l.offs = append([]int64{}, l.offs[:after]...)
		l.offs = append(l.offs, task.offsets[:end+1]...)
	}

	l.Unlock()

	e := &task.entries[end]
	task.cb(AppendResult{
		Term:  e.Term,
		Index: e.Index,
	}, nil)
}

func (l *Log) prepareAppend(ctx context.Context, entries []raftpb.Entry) error {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("raft.log.Log.prepareAppend() Start")
	defer span.AddEvent("raft.log.Log.prepareAppend() End")

	for i := 1; i < len(entries); i++ {
		entry, prev := &entries[i], &entries[i-1]
		var reason string
		if entry.Index != prev.Index+1 {
			reason = "Entries to append are discontinuous."
		} else if entry.Term < prev.Term {
			reason = "Term roll back."
		}
		if reason != "" {
			log.Warning(ctx, reason, map[string]interface{}{
				"node_id":        l.nodeID,
				"term":           entry.Term,
				"index":          entry.Index,
				"previous_term":  prev.Term,
				"previous_index": prev.Index,
			})
			return ErrBadEntry
		}
		if entry.Term != prev.Term {
			entry.PrevTerm = prev.Term
		}
	}
	return nil
}

func (l *Log) marshalEntries(entries []raftpb.Entry) ([][]byte, error) {
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
	return ents, nil
}

func (l *Log) appendToWAL(ctx context.Context, entries [][]byte, remark bool, cb walog.AppendCallback) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("raft.log.Log.appendToWAL() Start",
		trace.WithAttributes(attribute.Int("entry_count", len(entries)), attribute.Bool("remark", remark)))
	defer span.AddEvent("raft.log.Log.appendToWAL() End")

	if !remark {
		l.wal.Append(ctx, entries, walog.WithCallback(cb))
		return
	}

	l.wal.Append(ctx, entries, walog.WithCallback(func(ranges []walog.Range, err error) {
		if err != nil {
			panic(err)
		}

		l.Lock()

		// Record offset of first entry in WAL.
		off := ranges[0].SO
		old := l.offs[0]
		l.offs[0] = off

		l.Unlock()

		_ = l.wal.makeBarrier(context.TODO(), off, old)

		cb(ranges, err)
	}))
}
