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
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	defaultBlockSize        = 4 * 1024
	defaultFileSize         = 128 * 1024 * 1024
	defaultFlushTimeoutUs   = 200
	defaultAppendBufferSize = 64
	defaultFlushBufferSize  = 64
	defaultWakeupBufferSize = defaultFlushBufferSize * 2
)

type config struct {
	stream             *logStream
	pos                int64
	flushTimeout       time.Duration
	appendBufferSize   int
	callbackBufferSize int
	flushBufferSize    int
	wakeupBufferSize   int
	engine             io.Engine
}

func (c *config) blockSize() int64 {
	return c.stream.blockSize
}

func defaultWALConfig() config {
	cfg := config{
		stream: &logStream{
			blockSize: defaultBlockSize,
			fileSize:  defaultFileSize,
		},
		flushTimeout:       defaultFlushTimeoutUs * time.Microsecond,
		appendBufferSize:   defaultAppendBufferSize,
		callbackBufferSize: (defaultBlockSize + record.HeaderSize - 1) / record.HeaderSize,
		flushBufferSize:    defaultFlushBufferSize,
		wakeupBufferSize:   defaultWakeupBufferSize,
		engine:             defaultIOEngine(),
	}
	return cfg
}

type Option func(*config)

func makeConfig(dir string, opts ...Option) config {
	cfg := defaultWALConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.stream.dir = dir
	return cfg
}

func WithPosition(pos int64) Option {
	return func(cfg *config) {
		cfg.pos = pos
	}
}

func WithBlockSize(blockSize int64) Option {
	return func(cfg *config) {
		cfg.stream.blockSize = blockSize
		cfg.callbackBufferSize = int((blockSize + record.HeaderSize - 1) / record.HeaderSize)
	}
}

func WithFileSize(fileSize int64) Option {
	return func(cfg *config) {
		cfg.stream.fileSize = fileSize
	}
}

func WithFlushTimeout(d time.Duration) Option {
	return func(cfg *config) {
		cfg.flushTimeout = d
	}
}

func WithAppendBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.appendBufferSize = size
	}
}

func WithCallbackBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.callbackBufferSize = size
	}
}

func WithFlushBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.flushBufferSize = size
	}
}

func WithWakeupBufferSize(size int) Option {
	return func(cfg *config) {
		cfg.wakeupBufferSize = size
	}
}

func WithIOEngine(engine io.Engine) Option {
	return func(cfg *config) {
		cfg.engine = engine
	}
}
