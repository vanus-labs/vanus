// Copyright 2023 Linkall Inc.
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

package block

import (
	// this project.
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

const (
	defaultRaftExecutorParallel      = 4
	defaultAppendExecutorParallel    = 4
	defaultCommitExecutorParallel    = 4
	defaultPersistExecutorParallel   = 4
	defaultApplyExecutorParallel     = 4
	defaultTransportExecutorParallel = 4
)

type executorConfig struct {
	raftExecutorParallel      int
	appendExecutorParallel    int
	commitExecutorParallel    int
	persistExecutorParallel   int
	applyExecutorParallel     int
	transportExecutorParallel int
}

type config struct {
	stateStore *meta.SyncStore
	hintStore  *meta.AsyncStore

	walOpts     []walog.Option
	executorCfg executorConfig

	leaderLis LeaderChangedListener
	appendLis EntryAppendedListener
}

func defaultConfig() config {
	cfg := config{
		executorCfg: executorConfig{
			raftExecutorParallel:      defaultRaftExecutorParallel,
			appendExecutorParallel:    defaultAppendExecutorParallel,
			commitExecutorParallel:    defaultCommitExecutorParallel,
			persistExecutorParallel:   defaultPersistExecutorParallel,
			applyExecutorParallel:     defaultApplyExecutorParallel,
			transportExecutorParallel: defaultTransportExecutorParallel,
		},
	}
	return cfg
}

type Option func(*config)

func makeConfig(opts ...Option) config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.stateStore == nil { //nolint:staticcheck // todo
		// TODO(james.yin)
	}
	if cfg.hintStore == nil { //nolint:staticcheck // todo
		// TODO(james.yin)
	}
	return cfg
}

func WithStateStore(stateStore *meta.SyncStore) Option {
	return func(cfg *config) {
		cfg.stateStore = stateStore
	}
}

func WithHintStore(hintStore *meta.AsyncStore) Option {
	return func(cfg *config) {
		cfg.hintStore = hintStore
	}
}

func WithWALOptions(opts ...walog.Option) Option {
	return func(cfg *config) {
		cfg.walOpts = opts
	}
}

func WithRaftExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.raftExecutorParallel = parallel
	}
}

func WithAppendExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.appendExecutorParallel = parallel
	}
}

func WithCommitExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.commitExecutorParallel = parallel
	}
}

func WithPersistExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.persistExecutorParallel = parallel
	}
}

func WithApplyExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.applyExecutorParallel = parallel
	}
}

func WithTransportExecutorParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.executorCfg.transportExecutorParallel = parallel
	}
}

func WithLeaderChangedListener(lis LeaderChangedListener) Option {
	return func(cfg *config) {
		cfg.leaderLis = lis
	}
}

func WithEntryAppendedListener(lis EntryAppendedListener) Option {
	return func(cfg *config) {
		cfg.appendLis = lis
	}
}
