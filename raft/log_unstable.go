// Copyright 2015 The etcd Authors
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

package raft

import pb "github.com/vanus-labs/vanus/raft/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot
	// all entries that have not yet been written to storage.
	entries []pb.Entry
	offset  uint64

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any.
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

func (u *unstable) stableTo(i, t uint64) bool {
	gt, ok := u.maybeTerm(i)
	if !ok {
		// Unstable entry missing. Ignore.
		u.logger.Infof("entry at index %d missing from unstable log; ignoring", i)
		return false
	}
	if i < u.offset {
		// Index matched unstable snapshot, not unstable entry. Ignore.
		u.logger.Infof("entry at index %d matched unstable snapshot; ignoring", i)
		return false
	}
	if gt != t {
		// Term mismatch between unstable entry and specified entry. Ignore.
		// This is possible if part or all of the unstable log was replaced
		// between that time that a set of entries started to be written to
		// stable storage and when they finished.
		u.logger.Infof("entry at (index,term)=(%d,%d) mismatched with entry at (%d,%d) in unstable log; ignoring", i, t, i, gt)
		return false
	}
	u.entries = u.entries[i+1-u.offset:]
	u.offset = i + 1
	u.shrinkEntriesArray()
	return true
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

func (u *unstable) stableSnapTo(i uint64) bool {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
		return true
	}
	return false
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

func (u *unstable) truncateAndAppend(ents []pb.Entry) (li uint64, truncated bool) {
	start := ents[0].Index
	switch {
	case start == u.offset+uint64(len(u.entries)):
		// start is the next index in the u.entries, so append directly.
		u.entries = append(u.entries, ents...)
	case start <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", start)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries.
		u.offset = start
		u.entries = ents
		truncated = true
	default:
		// truncate to start and copy to u.entries then append.
		u.logger.Infof("truncate the unstable entries before index %d", start)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, start)...)
		u.entries = append(u.entries, ents...)
		truncated = true
	}
	li = ents[len(ents)-1].Index
	return
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
