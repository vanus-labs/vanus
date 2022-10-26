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

package option

import (
	"time"

	"github.com/linkall-labs/vanus/client/pkg/api"
)

func WithWritePolicy(policy api.WritePolicy) api.WriteOption {
	return func(options *api.WriteOptions) {
		options.Policy = policy
	}
}

func WithOneway() api.WriteOption {
	return func(options *api.WriteOptions) {
		options.Oneway = true
	}
}

func WithBatchSize(size int) api.ReadOption {
	return func(options *api.ReadOptions) {
		options.BatchSize = size
	}
}

func WithPollingTimeout(d time.Duration) api.ReadOption {
	return func(options *api.ReadOptions) {
		if d <= 0 {
			options.PollingTimeout = 0
		} else {
			options.PollingTimeout = d.Milliseconds()
		}
	}
}

func WithDisablePolling() api.ReadOption {
	return func(options *api.ReadOptions) {
		options.PollingTimeout = 0
	}
}

func WithReadPolicy(policy api.ReadPolicy) api.ReadOption {
	return func(options *api.ReadOptions) {
		options.Policy = policy
	}
}

func WithLogPolicy(policy api.LogPolicy) api.LogOption {
	return func(options *api.LogOptions) {
		options.Policy = policy
	}
}
