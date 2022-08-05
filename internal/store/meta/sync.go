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
	"errors"
	"time"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	storecfg "github.com/linkall-labs/vanus/internal/store"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	runSnapshotInterval = 30 * time.Second
)

type SyncStore struct {
	store

	snapshotc chan struct{}
	donec     chan struct{}
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
		donec:     make(chan struct{}),
	}

	go s.runSnapshot()

	return s
}

func (s *SyncStore) Close() {
	// Close WAL.
	s.wal.Close()
	s.wal.Wait()

	// NOTE: Can not close the snapshotc before close the WAL,
	// because write to snapshotc in callback of WAL append.
	close(s.snapshotc)
	<-s.donec
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
	// Use callbacks for ordering guarantees.
	s.wal.AppendOne(entry, walog.WithCallback(func(re walog.Result) {
		if re.Err != nil {
			ch <- re.Err
			return
		}

		// Update state.
		s.mu.Lock()
		_ = kvs.Range(func(key []byte, value interface{}) error {
			if value == deletedMark {
				s.committed.Remove(key)
			} else {
				s.committed.Set(key, value)
			}
			return nil
		})
		s.version = re.Range().EO
		s.mu.Unlock()

		close(ch)

		select {
		case s.snapshotc <- struct{}{}:
		default:
		}
	}))
	err = <-ch

	// Convert ErrClosed.
	if err != nil && errors.Is(err, walog.ErrClosed) {
		return ErrClosed
	}

	return err
}

func (s *SyncStore) runSnapshot() {
	ticker := time.NewTicker(runSnapshotInterval)
	defer func() {
		ticker.Stop()
		close(s.donec)
	}()

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

func RecoverSyncStore(cfg storecfg.SyncStoreConfig, walDir string) (*SyncStore, error) {
	committed, snapshot, err := recoverLatestSnapshot(walDir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	opts := append([]walog.Option{
		walog.FromPosition(snapshot),
		walog.WithRecoveryCallback(func(data []byte, r walog.Range) error {
			err2 := defaultCodec.Unmarshal(data, func(key []byte, value interface{}) error {
				set(committed, key, value)
				return nil
			})
			if err2 != nil {
				return err2
			}
			version = r.EO
			return nil
		}),
	}, cfg.WAL.Options()...)
	wal, err := walog.Open(walDir, opts...)
	if err != nil {
		return nil, err
	}

	return newSyncStore(wal, committed, version, snapshot), nil
}
