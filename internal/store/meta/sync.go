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
	runSnapshotInterval = 30 * time.Second
)

type SyncStore struct {
	store

	snapshotc chan struct{}
}

func newSyncStore(wal *walog.WAL, committed *skiplist.SkipList, version, snapshot int64) *SyncStore {
	s := &SyncStore{
		store: store{
			committed: committed,
			version:   version,
			wal:       wal,
			snapshot:  snapshot,
			marshaler: defaultCodec,
		},
		snapshotc: make(chan struct{}, 1),
	}

	go s.runSnapshot()

	return s
}

func (s *SyncStore) Stop() {
	// TODO(james.yin): stop WAL
}

func (s *SyncStore) Load(key []byte) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.load(key)
}

func (s *SyncStore) Store(key []byte, value interface{}) {
	if err := s.set(KVRange(key, value)); err != nil {
		panic(err)
	}
}

func (s *SyncStore) BatchStore(kvs Ranger) {
	if err := s.set(kvs); err != nil {
		panic(err)
	}
}

func (s *SyncStore) Delete(key []byte) {
	if err := s.set(KVRange(key, deletedMark)); err != nil {
		panic(err)
	}
}

func (s *SyncStore) set(kvs Ranger) error {
	entry, err := s.marshaler.Marshal(kvs)
	if err != nil {
		return err
	}

	ch := make(chan error, 1)
	s.wal.AppendOne(entry, walog.WithCallback(func(re walog.Result) {
		if re.Err != nil {
			ch <- re.Err
			return
		}

		// Update state.
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			_ = kvs.Range(func(key []byte, value interface{}) error {
				if value == deletedMark {
					s.committed.Remove(key)
				} else {
					s.committed.Set(key, value)
				}
				return nil
			})

			s.version = re.Offset()
		}()

		close(ch)

		select {
		case s.snapshotc <- struct{}{}:
		default:
		}
	}))
	return <-ch
}

func (s *SyncStore) runSnapshot() {
	ticker := time.NewTicker(runSnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case _, ok := <-s.snapshotc:
			if !ok {
				return
			}
		case <-ticker.C:
		}
		s.tryCreateSnapshot()
	}
}

func RecoverSyncStore(walDir string) (*SyncStore, error) {
	committed, snapshot, err := recoverLatestSnopshot(walDir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	wal, err := walog.RecoverWithVisitor(walDir, snapshot, func(data []byte, offset int64) error {
		err2 := defaultCodec.Unmarshl(data, func(key []byte, value interface{}) error {
			set(committed, key, value)
			return nil
		})
		if err2 != nil {
			return err2
		}
		version = offset
		return nil
	})
	if err != nil {
		return nil, err
	}

	return newSyncStore(wal, committed, version, snapshot), nil
}
