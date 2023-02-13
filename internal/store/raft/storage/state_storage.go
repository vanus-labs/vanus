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
	"fmt"
	"sync/atomic"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/meta"
)

type stateStorage struct {
	stateStore *meta.SyncStore
	hintStore  *meta.AsyncStore

	prevHardSt raftpb.HardState
	prevConfSt raftpb.ConfState
	prevApply  uint64

	hsKey  []byte
	offKey []byte
	csKey  []byte
	appKey []byte
}

// InitialState returns the saved HardState and ConfState information.
func (s *Storage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return s.prevHardSt, s.prevConfSt, nil
}

// HardState returns the saved HardState.
// NOTE: HardState.Commit will always be 0, don't use it.
func (s *Storage) HardState() raftpb.HardState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return raftpb.HardState{
		Term: s.prevHardSt.Term,
		Vote: s.prevHardSt.Vote,
	}
}

// SetHardState saves the current HardState.
func (s *Storage) SetHardState(ctx context.Context, hs raftpb.HardState, cb meta.StoreCallback) {
	data, err := hs.Marshal()
	if err != nil {
		cb(err)
		return
	}
	s.stateStore.Store(ctx, s.hsKey, data, cb)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.prevHardSt.Term = hs.Term
	s.prevHardSt.Vote = hs.Vote
}

func (s *Storage) Commit() uint64 {
	return atomic.LoadUint64(&s.prevHardSt.Commit)
}

func (s *Storage) SetCommit(ctx context.Context, commit uint64) {
	s.hintStore.Store(ctx, s.offKey, commit)
	atomic.StoreUint64(&s.prevHardSt.Commit, commit)
}

func (s *Storage) SetConfState(ctx context.Context, cs raftpb.ConfState, cb meta.StoreCallback) {
	data, err := cs.Marshal()
	if err != nil {
		cb(err)
		return
	}
	s.stateStore.Store(ctx, s.csKey, data, cb)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.prevConfSt = cs
}

func (s *Storage) Applied() uint64 {
	return atomic.LoadUint64(&s.prevApply)
}

func (s *Storage) SetApplied(ctx context.Context, app uint64) {
	s.hintStore.Store(ctx, s.appKey, app)
	atomic.StoreUint64(&s.prevApply, app)
}

func HardStateKey(id uint64) string {
	return fmt.Sprintf("block/%020d/hardState", id)
}

func CommitKey(id uint64) string {
	return fmt.Sprintf("block/%020d/commit", id)
}

func ConfStateKey(id uint64) string {
	return fmt.Sprintf("block/%020d/confState", id)
}

func ApplyKey(id uint64) string {
	return fmt.Sprintf("block/%020d/applied", id)
}
