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

package vsb

import (
	// standard libraries.
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ioengine "github.com/linkall-labs/vanus/internal/store/io/engine"
	"github.com/linkall-labs/vanus/internal/store/io/engine/psync"
)

const (
	defaultFlushBatchSize = 4 * 1024
	defaultFlushDelayTime = 3 * time.Millisecond
)

type config struct {
	engine         ioengine.Interface
	flushBatchSize int
	flushDelayTime time.Duration
	lis            block.ArchivedListener
}

func defaultConfig() config {
	cfg := config{
		flushBatchSize: defaultFlushBatchSize,
		flushDelayTime: defaultFlushDelayTime,
	}
	return cfg
}

type Option func(*config)

func makeConfig(opts ...Option) config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.engine == nil {
		cfg.engine = defaultIOEngine()
	}
	return cfg
}

func defaultIOEngine() ioengine.Interface {
	return psync.New()
}

func WithIOEngine(engine ioengine.Interface) Option {
	return func(cfg *config) {
		cfg.engine = engine
	}
}

func WithFlushBatchSize(size int) Option {
	return func(cfg *config) {
		cfg.flushBatchSize = size
	}
}

func WithFlushDelayTime(d time.Duration) Option {
	return func(cfg *config) {
		cfg.flushDelayTime = d
	}
}

func WithArchivedListener(lis block.ArchivedListener) Option {
	return func(cfg *config) {
		cfg.lis = lis
	}
}
