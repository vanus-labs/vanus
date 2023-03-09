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

package policy

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/vanus-labs/vanus/client/pkg/api"
)

var _ api.WritePolicy = (*roundRobinWritePolicy)(nil)

func NewRoundRobinWritePolicy(eb api.Eventbus) api.WritePolicy {
	return &roundRobinWritePolicy{
		bus: eb,
	}
}

type roundRobinWritePolicy struct {
	bus    api.Eventbus
	idx    uint64
	cached []api.Eventlog
	mutex  sync.Mutex
}

func (w *roundRobinWritePolicy) Type() api.PolicyType {
	return api.RoundRobin
}

func (w *roundRobinWritePolicy) NextLog(ctx context.Context) (api.Eventlog, error) {
	for {
		logs, err := w.bus.ListLog(ctx)
		if err != nil {
			return nil, err
		}
		if len(logs) == 0 {
			continue
		}

		if len(logs) == len(w.cached) {
			logs = w.cached
		} else {
			w.mutex.Lock()
			sort.Slice(logs, func(i, j int) bool {
				return logs[i].ID() > logs[j].ID()
			})
			w.cached = logs
			w.mutex.Unlock()
		}

		l := len(logs)
		i := atomic.AddUint64(&w.idx, 1) % uint64(l)
		return logs[i], nil
	}
}

var _ api.ReadPolicy = (*roundRobinReadPolicy)(nil)

func NewRoundRobinReadPolicy(eb api.Eventbus, fromWhere api.ConsumeFromWhere) *roundRobinReadPolicy {
	p := &roundRobinReadPolicy{
		bus:    eb,
		offset: 0,
	}
	log, err := p.NextLog(context.Background())
	if err != nil {
		return p
	}
	if fromWhere == api.ConsumeFromWhereEarliest {
		p.offset, err = log.EarliestOffset(context.Background())
		if err != nil {
			p.offset = 0
		}
	} else if fromWhere == api.ConsumeFromWhereLatest {
		p.offset, err = log.LatestOffset(context.Background())
		if err != nil {
			p.offset = 0
		}
	}
	return p
}

type roundRobinReadPolicy struct {
	bus    api.Eventbus
	idx    uint64
	offset int64
	cached []api.Eventlog
	mutex  sync.Mutex
}

func (r *roundRobinReadPolicy) Type() api.PolicyType {
	return api.RoundRobin
}

func (r *roundRobinReadPolicy) NextLog(ctx context.Context) (api.Eventlog, error) {
	for {
		logs, err := r.bus.ListLog(ctx)
		if err != nil {
			return nil, err
		}

		if len(logs) == 0 {
			continue
		}

		if len(logs) == len(r.cached) {
			logs = r.cached
		} else {
			r.mutex.Lock()
			sort.Slice(logs, func(i, j int) bool {
				return logs[i].ID() > logs[j].ID()
			})
			r.cached = logs
			r.mutex.Unlock()
		}

		l := len(logs)
		i := atomic.AddUint64(&r.idx, 1) % uint64(l)
		return logs[i], nil
	}
}

func (r *roundRobinReadPolicy) Offset() int64 {
	return atomic.LoadInt64(&r.offset)
}

func (r *roundRobinReadPolicy) Forward(diff int) {
	atomic.AddInt64(&r.offset, int64(diff))
}

var _ api.ReadPolicy = (*manuallyReadPolicy)(nil)

func NewManuallyReadPolicy(log api.Eventlog, offset int64) *manuallyReadPolicy {
	return &manuallyReadPolicy{
		log:    log,
		offset: offset,
	}
}

type manuallyReadPolicy struct {
	log    api.Eventlog
	offset int64
}

func (r manuallyReadPolicy) Type() api.PolicyType {
	return api.Manually
}

func (r manuallyReadPolicy) NextLog(ctx context.Context) (api.Eventlog, error) {
	return r.log, nil
}

func (r manuallyReadPolicy) Offset() int64 {
	return atomic.LoadInt64(&r.offset)
}

func (r *manuallyReadPolicy) Forward(diff int) {
	atomic.AddInt64(&r.offset, int64(diff))
}

var _ api.LogPolicy = (*readOnlyPolicy)(nil)

func NewReadOnlyPolicy() api.LogPolicy {
	return &readOnlyPolicy{}
}

type readOnlyPolicy struct{}

func (w *readOnlyPolicy) AccessMode() api.PolicyType {
	return api.ReadOnly
}

var _ api.LogPolicy = (*readWritePolicy)(nil)

func NewReadWritePolicy() api.LogPolicy {
	return &readWritePolicy{}
}

type readWritePolicy struct{}

func (w *readWritePolicy) AccessMode() api.PolicyType {
	return api.ReadWrite
}
