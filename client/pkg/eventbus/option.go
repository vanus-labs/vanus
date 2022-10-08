// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventbus

import (
	"time"
)

const (
	DefaultPollingTimeout = 3000 // in milliseconds.
)

type WriteOption func(*writeOptions)

type writeOptions struct {
	policy WritePolicy
	oneway bool
}

func WithWritePolicy(policy WritePolicy) WriteOption {
	return func(options *writeOptions) {
		options.policy = policy
	}
}

func WithOneway() WriteOption {
	return func(options *writeOptions) {
		options.oneway = true
	}
}

type ReadOption func(*readOptions)

type readOptions struct {
	batchSize      int
	pollingTimeout int64
	policy         ReadPolicy
}

func WithBatchSize(size int) ReadOption {
	return func(options *readOptions) {
		options.batchSize = size
	}
}

func WithPollingTimeout(d time.Duration) ReadOption {
	return func(options *readOptions) {
		if d <= 0 {
			options.pollingTimeout = 0
		} else {
			options.pollingTimeout = d.Milliseconds()
		}
	}
}

func WithDisablePolling() ReadOption {
	return func(options *readOptions) {
		options.pollingTimeout = 0
	}
}

func WithReadPolicy(policy ReadPolicy) ReadOption {
	return func(options *readOptions) {
		options.policy = policy
	}
}

type LogOption func(*logOptions)

type logOptions struct {
	policy LogPolicy
}

func WithLogPolicy(policy LogPolicy) LogOption {
	return func(options *logOptions) {
		options.policy = policy
	}
}
