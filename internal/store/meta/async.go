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
	"time"

	// third-party libraries.
	"github.com/huandu/skiplist"

	// this project.
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

const (
	runCommitInterval = 3 * time.Second
)

type AsyncStore struct {
	store

	pending *skiplist.SkipList

	commitC chan struct{}
	closeC  chan struct{}
	doneC   chan struct{}
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
		commitC: make(chan struct{}, 1),
		closeC:  make(chan struct{}),
		doneC:   make(chan struct{}),
	}

	go s.runCommit()

	return s
}

func (s *AsyncStore) Close() {
	s.mu.Lock()
	close(s.closeC)
	s.mu.Unlock()

	<-s.doneC

	// Close WAL.
	s.wal.Close()
	s.wal.Wait()
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

func (s *AsyncStore) Store(_ context.Context, key []byte, value interface{}) {
	_ = s.set(KVRange(key, value))
}

func (s *AsyncStore) BatchStore(_ context.Context, kvs Ranger) {
	_ = s.set(kvs)
}

func (s *AsyncStore) Delete(key []byte) {
	_ = s.set(KVRange(key, deletedMark))
}

func (s *AsyncStore) BatchDelete(keys [][]byte) {
	_ = s.set(&deleteRange{keys})
}

func (s *AsyncStore) set(kvs Ranger) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closeC:
		return ErrClosed
	default:
	}

	err := kvs.Range(func(key []byte, value interface{}) error {
		s.pending.Set(key, value)
		return nil
	})
	if err != nil {
		return err
	}

	s.tryCommit()

	return nil
}

func (s *AsyncStore) tryCommit() {
	if s.needCommit() {
		select {
		case s.commitC <- struct{}{}:
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
		close(s.doneC)
	}()

	for {
		select {
		case <-s.closeC:
			return
		case <-s.commitC:
		case <-ticker.C:
		}
		s.commit()
	}
}

func (s *AsyncStore) commit() {
	ctx := context.Background()
	defer s.tryCreateSnapshot()

	s.mu.Lock()

	if s.pending.Len() == 0 {
		s.mu.Unlock()
		return
	}

	// Marshal changed data.
	data, err := s.marshaler.Marshal(SkiplistRange(s.pending))
	if err != nil {
		panic(err)
	}

	// Update state.
	merge(s.committed, s.pending)
	s.pending.Init()

	s.mu.Unlock()

	// Write WAL.
	r, err := walog.DirectAppendOne(ctx, s.wal, data)
	if err != nil {
		panic(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.version = r.EO
}

func merge(dst, src *skiplist.SkipList) {
	for el := src.Front(); el != nil; el = el.Next() {
		set(dst, el.Key().([]byte), el.Value)
	}
}

func RecoverAsyncStore(ctx context.Context, dir string, opts ...walog.Option) (*AsyncStore, error) {
	committed, snapshot, err := recoverLatestSnapshot(ctx, dir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	opts = append([]walog.Option{
		walog.FromPosition(snapshot),
		walog.WithRecoveryCallback(func(data []byte, r walog.Range) error {
			m := skiplist.New(skiplist.Bytes)
			err2 := defaultCodec.Unmarshal(data, func(key []byte, value interface{}) error {
				m.Set(key, value)
				return nil
			})
			if err2 != nil {
				return err2
			}
			merge(committed, m)
			version = r.EO
			return nil
		}),
	}, opts...)
	wal, err := walog.Open(ctx, dir, opts...)
	if err != nil {
		return nil, err
	}

	return newAsyncStore(wal, committed, version, snapshot), nil
}
