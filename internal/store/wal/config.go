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

package wal

import (
	// standard libraries.
	"time"

	// this project.
	ioengine "github.com/linkall-labs/vanus/internal/store/io/engine"
	"github.com/linkall-labs/vanus/internal/store/io/stream"
	"github.com/linkall-labs/vanus/internal/store/io/zone/segmentedfile"
)

const (
	logFileExt              = ".log"
	defaultBlockSize        = 16 * 1024
	defaultFileSize         = 128 * 1024 * 1024
	defaultAppendBufferSize = 64
)

type config struct {
	pos              int64
	cb               OnEntryCallback
	blockSize        int
	fileSize         int64
	flushDelayTime   time.Duration // default: 3 * time.Millisecond
	appendBufferSize int
	engine           ioengine.Interface
}

func (cfg *config) segmentedFileOptions() []segmentedfile.Option {
	opts := []segmentedfile.Option{segmentedfile.WithExtension(logFileExt)}
	if cfg.fileSize != 0 {
		opts = append(opts, segmentedfile.WithSegmentSize(cfg.fileSize))
	}
	return opts
}

func (cfg *config) streamSchedulerOptions() []stream.Option {
	opts := []stream.Option{stream.WithCallbackParallel(1)}
	if cfg.blockSize != 0 {
		opts = append(opts, stream.WithFlushBatchSize(cfg.blockSize))
	}
	if cfg.flushDelayTime != 0 {
		opts = append(opts, stream.WithFlushDelayTime(cfg.flushDelayTime))
	}
	return opts
}

func defaultConfig() config {
	cfg := config{
		blockSize:        defaultBlockSize,
		fileSize:         defaultFileSize,
		appendBufferSize: defaultAppendBufferSize,
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

func FromPosition(pos int64) Option {
	return func(cfg *config) {
		cfg.pos = pos
	}
}

func WithRecoveryCallback(cb OnEntryCallback) Option {
	return func(cfg *config) {
		cfg.cb = cb
	}
}

func WithBlockSize(blockSize int) Option {
	return func(cfg *config) {
		cfg.blockSize = blockSize
	}
}

func WithFileSize(fileSize int64) Option {
	return func(cfg *config) {
		cfg.fileSize = fileSize
	}
}

func WithFlushDelayTime(d time.Duration) Option {
	return func(cfg *config) {
		cfg.flushDelayTime = d
	}
}

func WithAppendBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.appendBufferSize = size
	}
}

func WithIOEngine(engine ioengine.Interface) Option {
	return func(cfg *config) {
		cfg.engine = engine
	}
}
