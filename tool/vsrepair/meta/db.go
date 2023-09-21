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
	"encoding/binary"
	"errors"
	"path/filepath"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"
	"github.com/vanus-labs/vanus/server/store/meta"
	"github.com/vanus-labs/vanus/server/store/raft/storage"
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

var ErrNotFound = errors.New("not found")

type DB struct {
	metaStore   *meta.SyncStore
	offsetStore *meta.AsyncStore
}

type config struct {
	skipMetaStore bool
	readOnly      bool
}

type Option func(*config)

func SkipMetaStore() Option {
	return func(c *config) {
		c.skipMetaStore = true
	}
}

func ReadOnly() Option {
	return func(c *config) {
		c.readOnly = true
	}
}

func Open(volumeDir string, opts ...Option) (*DB, error) {
	ctx := context.Background()

	cfg := config{}
	for _, opt := range opts {
		opt(&cfg)
	}

	var metaStore *meta.SyncStore
	if !cfg.skipMetaStore {
		var err error
		var metaOpts []walog.Option
		if cfg.readOnly {
			metaOpts = append(metaOpts, walog.WithReadOnly())
		}
		metaStore, err = meta.RecoverSyncStore(ctx, filepath.Join(volumeDir, "meta"), metaOpts...)
		if err != nil {
			return nil, err
		}
	}

	var offsetOpts []walog.Option
	if cfg.readOnly {
		offsetOpts = append(offsetOpts, walog.WithReadOnly())
	}
	offsetStore, err := meta.RecoverAsyncStore(ctx, filepath.Join(volumeDir, "offset"), offsetOpts...)
	if err != nil {
		if !cfg.skipMetaStore {
			metaStore.Close(ctx)
		}
		return nil, err
	}

	return &DB{
		metaStore,
		offsetStore,
	}, nil
}

func (db *DB) Close() {
	if db.metaStore != nil {
		db.metaStore.Close(context.Background())
	}
	if db.offsetStore != nil {
		db.offsetStore.Close()
	}
}

type CompactInfo struct {
	Index uint64 `json:"Index"`
	Term  uint64 `json:"Term"`
}

type RaftDetail struct {
	ConfState raftpb.ConfState `json:"ConfState"`
	HardState raftpb.HardState `json:"HardState"`
	Commit    uint64           `json:"Commit"`
	Apply     uint64           `json:"Apply"`
	Compact   CompactInfo      `json:"Compact"`
}

func (db *DB) GetRaftDetail(node uint64) (d RaftDetail, err error) {
	d.Compact, err = db.GetCompact(node)
	if err != nil {
		return
	}

	d.ConfState, err = db.GetConfState(node)
	if err != nil && err != ErrNotFound { //nolint:errorlint // compare to ErrNotFound is ok.
		return
	}

	d.HardState, err = db.GetHardState(node)
	if err != nil && err != ErrNotFound { //nolint:errorlint // compare to ErrNotFound is ok.
		return
	}

	d.Commit, err = db.GetCommit(node)
	if err != nil && err != ErrNotFound { //nolint:errorlint // compare to ErrNotFound is ok.
		return
	}

	d.Apply, err = db.GetApply(node)
	if err != nil && err != ErrNotFound { //nolint:errorlint // compare to ErrNotFound is ok.
		return
	}

	return d, nil
}

func (db *DB) GetConfState(node uint64) (raftpb.ConfState, error) {
	csKey := []byte(storage.ConfStateKey(node))

	var cs raftpb.ConfState
	if v, exist := db.metaStore.Load(csKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("confState is not []byte")
		}
		if err := cs.Unmarshal(b); err != nil {
			return raftpb.ConfState{}, err
		}
	} else {
		return raftpb.ConfState{}, ErrNotFound
	}
	return cs, nil
}

func (db *DB) GetHardState(node uint64) (raftpb.HardState, error) {
	hsKey := []byte(storage.HardStateKey(node))

	var hs raftpb.HardState
	if v, exist := db.metaStore.Load(hsKey); exist {
		b, ok := v.([]byte)
		if !ok {
			panic("hardState is not []byte")
		}
		if err := hs.Unmarshal(b); err != nil {
			return raftpb.HardState{}, err
		}
	} else {
		return raftpb.HardState{}, ErrNotFound
	}

	// clear commit
	hs.Commit = 0

	return hs, nil
}

func (db *DB) PutHardState(node uint64, hs raftpb.HardState) error {
	hsKey := []byte(storage.HardStateKey(node))
	offKey := []byte(storage.CommitKey(node))

	data, err := hs.Marshal()
	if err != nil {
		return err
	}
	if err := meta.Store(context.Background(), db.metaStore, hsKey, data); err != nil {
		return err
	}
	db.offsetStore.Delete(offKey)
	return nil
}

func (db *DB) GetCommit(node uint64) (uint64, error) {
	offKey := []byte(storage.CommitKey(node))

	if v, exist := db.offsetStore.Load(offKey); exist {
		off, ok := v.(uint64)
		if !ok {
			panic("commit is not uint64")
		}
		return off, nil
	}
	return 0, ErrNotFound
}

func (db *DB) GetApply(node uint64) (uint64, error) {
	appKey := []byte(storage.ApplyKey(node))

	if v, exist := db.offsetStore.Load(appKey); exist {
		app, ok := v.(uint64)
		if !ok {
			panic("applied is not uint64")
		}
		return app, nil
	}
	return 0, ErrNotFound
}

func (db *DB) PutApply(node uint64, app uint64) error {
	appKey := []byte(storage.ApplyKey(node))

	db.offsetStore.Store(context.Background(), appKey, app)
	return nil
}

func (db *DB) GetCompact(node uint64) (CompactInfo, error) {
	comKey := []byte(storage.CompactKey(node))

	if v, exist := db.metaStore.Load(comKey); exist {
		com, ok := v.([]byte)
		if !ok {
			panic("compacted is not []byte")
		}
		info := CompactInfo{
			Index: binary.BigEndian.Uint64(com[0:8]),
			Term:  binary.BigEndian.Uint64(com[8:16]),
		}
		return info, nil
	}
	return CompactInfo{}, ErrNotFound
}

func (db *DB) PutCompact(node uint64, info CompactInfo) error {
	comKey := []byte(storage.CompactKey(node))

	var value [16]byte
	binary.BigEndian.PutUint64(value[0:8], info.Index)
	binary.BigEndian.PutUint64(value[8:16], info.Term)
	if err := meta.Store(context.Background(), db.metaStore, comKey, value[:]); err != nil {
		return err
	}
	return nil
}
