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

type EventbusOptions struct {
	Namespace string
	Name      string
	ID        uint64
}

func DefaultEventbusOptions() *EventbusOptions {
	return &EventbusOptions{}
}

type EventbusOption func(opt *EventbusOptions)

func WithName(namespace, name string) EventbusOption {
	return func(opt *EventbusOptions) {
		opt.Namespace = namespace
		opt.Name = name
	}
}

func WithID(id uint64) EventbusOption {
	return func(opt *EventbusOptions) {
		opt.ID = id
	}
}

type WriteOption func(*WriteOptions)

type WriteOptions struct {
	Policy WritePolicy
	Oneway bool
}

func (wo *WriteOptions) Apply(opts ...WriteOption) {
	for i := range opts {
		opts[i](wo)
	}
}

func (wo *WriteOptions) Copy() *WriteOptions {
	return &WriteOptions{
		Oneway: wo.Oneway,
		Policy: wo.Policy,
	}
}

type ReadOption func(*ReadOptions)

type ReadOptions struct {
	BatchSize      int
	PollingTimeout int64
	Policy         ReadPolicy
}

func (ro *ReadOptions) Apply(opts ...ReadOption) {
	for i := range opts {
		opts[i](ro)
	}
}

func (ro *ReadOptions) Copy() *ReadOptions {
	return &ReadOptions{
		BatchSize:      ro.BatchSize,
		PollingTimeout: ro.PollingTimeout,
		Policy:         ro.Policy,
	}
}

type LogOption func(*LogOptions)

type LogOptions struct {
	Policy LogPolicy
}
