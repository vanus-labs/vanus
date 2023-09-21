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

package stream

import "time"

const (
	defaultFlushBatchSize   = 16 * 1024
	defaultFlushDelayTime   = 3 * time.Millisecond
	defaultCallbackParallel = 1
)

type config struct {
	flushBatchSize   int
	flushDelayTime   time.Duration
	callbackParallel int
}

func defaultConfig() config {
	cfg := config{
		flushBatchSize:   defaultFlushBatchSize,
		flushDelayTime:   defaultFlushDelayTime,
		callbackParallel: defaultCallbackParallel,
	}
	return cfg
}

type Option func(*config)

func makeConfig(opts ...Option) config {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
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

func WithCallbackParallel(parallel int) Option {
	return func(cfg *config) {
		cfg.callbackParallel = parallel
	}
}
