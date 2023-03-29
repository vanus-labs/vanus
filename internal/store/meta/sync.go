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
	"context"
	"errors"
	"time"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

const (
	runSnapshotInterval = 30 * time.Second
)

type SyncStore struct {
	store

	snapshotC chan struct{}
	doneC     chan struct{}
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
		snapshotC: make(chan struct{}, 1),
		doneC:     make(chan struct{}),
	}

	go s.runSnapshot()

	return s
}

func (s *SyncStore) Close(_ context.Context) {
	// Close WAL.
	s.wal.Close()
	s.wal.Wait()

	// NOTE: Can not close the snapshotC before close the WAL,
	// because write to snapshotC in callback of WAL append.
	close(s.snapshotC)
	<-s.doneC
}

func (s *SyncStore) Load(key []byte) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.load(key)
}

func (s *SyncStore) Range(begin, end []byte, cb RangeCallback) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for el := s.committed.Find(begin); el != nil; el = el.Next() {
		if skiplist.Bytes.Compare(el.Key(), end) >= 0 {
			break
		}
		if err := cb(el.Key().([]byte), el.Value); err != nil {
			return err
		}
	}
	return nil
}

type StoreCallback = func(error)

func (s *SyncStore) Store(ctx context.Context, key []byte, value interface{}, cb StoreCallback) {
	s.set(ctx, KVRange(key, value), cb)
}

func (s *SyncStore) BatchStore(ctx context.Context, kvs Ranger, cb StoreCallback) {
	s.set(ctx, kvs, cb)
}

func (s *SyncStore) Delete(ctx context.Context, key []byte, cb StoreCallback) {
	s.set(ctx, KVRange(key, deletedMark), cb)
}

func (s *SyncStore) BatchDelete(ctx context.Context, keys [][]byte, cb StoreCallback) {
	s.set(ctx, &deleteRange{keys}, cb)
}

func (s *SyncStore) set(ctx context.Context, kvs Ranger, cb StoreCallback) {
	entry, err := s.marshaler.Marshal(kvs)
	if err != nil {
		cb(err)
		return
	}

	// Use callbacks for ordering guarantees.
	s.wal.AppendOne(ctx, entry, func(r walog.Range, err error) {
		if err != nil {
			// Convert ErrClosed.
			if errors.Is(err, walog.ErrClosed) {
				err = ErrClosed
			}
			cb(err)
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
		s.version = r.EO
		s.mu.Unlock()

		cb(nil)

		select {
		case s.snapshotC <- struct{}{}:
		default:
		}
	})
}

func (s *SyncStore) runSnapshot() {
	ticker := time.NewTicker(runSnapshotInterval)
	defer func() {
		ticker.Stop()
		close(s.doneC)
	}()
	for {
		select {
		case _, ok := <-s.snapshotC:
			if !ok {
				return
			}
		case <-ticker.C:
		}
		s.tryCreateSnapshot()
	}
}

func RecoverSyncStore(ctx context.Context, dir string, opts ...walog.Option) (*SyncStore, error) {
	committed, snapshot, err := recoverLatestSnapshot(ctx, dir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	opts = append([]walog.Option{
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
	}, opts...)
	wal, err := walog.Open(ctx, dir, opts...)
	if err != nil {
		return nil, err
	}

	return newSyncStore(wal, committed, version, snapshot), nil
}

type storeFuture chan error

func newStoreFuture() storeFuture {
	return make(storeFuture, 1)
}

func (sf storeFuture) onStored(err error) {
	if err != nil {
		sf <- err
	}
	close(sf)
}

func (sf storeFuture) wait() error {
	return <-sf
}

func Store(ctx context.Context, s *SyncStore, key []byte, value interface{}) error {
	future := newStoreFuture()
	s.Store(ctx, key, value, future.onStored)
	return future.wait()
}

func BatchStore(ctx context.Context, s *SyncStore, kvs Ranger) error {
	future := newStoreFuture()
	s.BatchStore(ctx, kvs, future.onStored)
	return future.wait()
}

func Delete(ctx context.Context, s *SyncStore, key []byte) error {
	future := newStoreFuture()
	s.Delete(ctx, key, future.onStored)
	return future.wait()
}

func BatchDelete(ctx context.Context, s *SyncStore, keys [][]byte) error {
	future := newStoreFuture()
	s.BatchDelete(ctx, keys, future.onStored)
	return future.wait()
}
