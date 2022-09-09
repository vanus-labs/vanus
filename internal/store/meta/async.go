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
	"github.com/linkall-labs/vanus/observability/tracing"
	oteltracer "go.opentelemetry.io/otel/trace"

	// this project.
	storecfg "github.com/linkall-labs/vanus/internal/store"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	runCommitInterval = 3 * time.Second
)

type AsyncStore struct {
	store

	pending *skiplist.SkipList

	commitc chan struct{}
	closec  chan struct{}
	donec   chan struct{}
}

func newAsyncStore(
	ctx context.Context,
	wal *walog.WAL,
	committed *skiplist.SkipList,
	version, snapshot int64) *AsyncStore {
	_, span := tracing.Start(ctx, "store.meta.async", "newAsyncStore")
	defer span.End()

	s := &AsyncStore{
		store: store{
			committed: committed,
			version:   version,
			wal:       wal,
			snapshot:  snapshot,
			marshaler: defaultCodec,
			tracer:    tracing.NewTracer("store.meta.async", oteltracer.SpanKindInternal),
		},
		pending: skiplist.New(skiplist.Bytes),
		commitc: make(chan struct{}, 1),
		closec:  make(chan struct{}),
		donec:   make(chan struct{}),
	}

	go s.runCommit() //nolint:contextcheck // wrong advice

	return s
}

func (s *AsyncStore) Close() {
	s.mu.Lock()
	close(s.closec)
	s.mu.Unlock()

	<-s.donec

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

func (s *AsyncStore) Store(key []byte, value interface{}) {
	_ = s.set(KVRange(key, value))
}

func (s *AsyncStore) BatchStore(kvs Ranger) {
	_ = s.set(kvs)
}

func (s *AsyncStore) Delete(key []byte) {
	_ = s.set(KVRange(key, deletedMark))
}

func (s *AsyncStore) set(kvs Ranger) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.closec:
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
		case <-s.closec:
			return
		case <-s.commitc:
		case <-ticker.C:
		}
		s.commit()
	}
}

func (s *AsyncStore) commit() {
	ctx, span := s.tracer.Start(context.Background(), "commit")
	defer func() {
		s.tryCreateSnapshot(ctx)
		span.End()
	}()

	if s.pending.Len() == 0 {
		return
	}

	// Write WAL.
	s.mu.RLock()
	data, err := s.marshaler.Marshal(SkiplistRange(s.pending))
	s.mu.RUnlock()
	if err != nil {
		panic(err)
	}
	r, err := s.wal.AppendOne(ctx, data, walog.WithoutBatching()).Wait()
	if err != nil {
		panic(err)
	}

	// Update state.
	s.mu.Lock()
	defer s.mu.Unlock()
	merge(s.committed, s.pending)
	s.version = r.EO
	s.pending.Init()
}

func merge(dst, src *skiplist.SkipList) {
	for el := src.Front(); el != nil; el = el.Next() {
		set(dst, el.Key().([]byte), el.Value)
	}
}

func RecoverAsyncStore(ctx context.Context, cfg storecfg.AsyncStoreConfig, walDir string) (*AsyncStore, error) {
	ctx, span := tracing.Start(ctx, "store.meta.async", "newAsyncStore")
	defer span.End()
	committed, snapshot, err := recoverLatestSnapshot(ctx, walDir, defaultCodec)
	if err != nil {
		return nil, err
	}

	version := snapshot
	opts := append([]walog.Option{
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
	}, cfg.WAL.Options()...)
	wal, err := walog.Open(ctx, walDir, opts...)
	if err != nil {
		return nil, err
	}

	return newAsyncStore(ctx, wal, committed, version, snapshot), nil
}
