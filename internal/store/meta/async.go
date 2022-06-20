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

package meta

import (
	// standard libraries.
	"time"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	runCommitInterval = 3 * time.Second
)

type AsyncStore struct {
	store

	pending *skiplist.SkipList

	commitc chan struct{}
	donec   chan struct{}
}

func newAsyncStore(wal *walog.WAL, committed *skiplist.SkipList, version, snapshot int64) *AsyncStore {
	s := &AsyncStore{
		store: store{
			committed: committed,
			version:   version,
			wal:       wal,
			snapshot:  snapshot,
			marshaler: defaultCodec,
		},
		pending: skiplist.New(skiplist.Bytes),
		commitc: make(chan struct{}, 1),
		donec:   make(chan struct{}),
	}

	go s.runCommit()

	return s
}

func (s *AsyncStore) Stop() {
	// TODO(james.yin): stop WAL
	close(s.commitc)
	<-s.donec
}

func (s *AsyncStore) Load(key []byte) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.pending.GetValue(key); ok {
		if v == deletedMark {
			return nil, false
		}
		return v, true
	}
	return s.load(key)
}

func (s *AsyncStore) Store(key []byte, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending.Set(key, value)
	s.tryCommit()
}

func (s *AsyncStore) Delete(key []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending.Set(key, deletedMark)
	s.tryCommit()
}

func (s *AsyncStore) tryCommit() {
	if s.needCommit() {
		select {
		case s.commitc <- struct{}{}:
		default:
		}
	}
}

func (s *AsyncStore) needCommit() bool {
	// TODO(james.yin): commit condition
	return false
}

func (s *AsyncStore) runCommit() {
	ticker := time.NewTicker(runCommitInterval)
	defer func() {
		ticker.Stop()
		s.commit()
		close(s.donec)
	}()

	for {
		select {
		case _, ok := <-s.commitc:
			if !ok {
				return
			}
		case <-ticker.C:
		}
		s.commit()
	}
}

func (s *AsyncStore) commit() {
	defer s.tryCreateSnapshot()

	if s.pending.Len() == 0 {
		return
	}

	// Write WAL.
	data, err := s.marshaler.Marshal(SkiplistRange(s.pending))
	if err != nil {
		panic(err)
	}
	offset, err := s.wal.AppendOne(data, walog.WithoutBatching()).Wait()
	if err != nil {
		panic(err)
	}

	// Update state.
	s.mu.Lock()
	defer s.mu.Unlock()
	merge(s.committed, s.pending)
	s.version = offset
	s.pending.Init()
}

func merge(dst, src *skiplist.SkipList) {
	for el := src.Front(); el != nil; el = el.Next() {
		set(dst, el.Key().([]byte), el.Value)
	}
}

func RecoverAsyncStore(walDir string) (*AsyncStore, error) {
	committed, snapshot, err := recoverLatestSnopshot(walDir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	wal, err := walog.RecoverWithVisitor(walDir, snapshot, func(data []byte, offset int64) error {
		m := skiplist.New(skiplist.Bytes)
		err2 := defaultCodec.Unmarshal(data, func(key []byte, value interface{}) error {
			m.Set(key, value)
			return nil
		})
		if err2 != nil {
			return err2
		}
		merge(committed, m)
		version = offset
		return nil
	})
	if err != nil {
		return nil, err
	}

	return newAsyncStore(wal, committed, version, snapshot), nil
}
