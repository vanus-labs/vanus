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

package api

const (
	DefaultPollingTimeout = 3000 // in milliseconds.
)

type WriteOption func(*WriteOptions)

type WriteOptions struct {
	Policy WritePolicy
	Oneway bool
}

func (opts *WriteOptions) Copy() *WriteOptions {
	return &WriteOptions{
		Oneway: opts.Oneway,
		Policy: opts.Policy,
	}
}

type ReadOption func(*ReadOptions)

type ReadOptions struct {
	BatchSize      int
	PollingTimeout int64
	Policy         ReadPolicy
}

func (opts *ReadOptions) Copy() *ReadOptions {
	return &ReadOptions{
		BatchSize:      opts.BatchSize,
		PollingTimeout: opts.PollingTimeout,
		Policy:         opts.Policy,
	}
}

type LogOption func(*LogOptions)

type LogOptions struct {
	Policy LogPolicy
}
