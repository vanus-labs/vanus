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
	"context"
	"sync/atomic"

	"github.com/linkall-labs/vanus/client/pkg/eventlog"
)

type policyType string

const (
	RoundRobin = policyType("round_robin")
	Manually   = policyType("manually")
	Weight     = policyType("weight")
	ReadOnly   = policyType("readonly")
	ReadWrite  = policyType("readwrite")
)

type consumeFromWhere string

var (
	ConsumeFromWhereEarliest = consumeFromWhere("earliest")
	ConsumeFromWhereLatest   = consumeFromWhere("latest")
)

type WritePolicy interface {
	Type() policyType
	NextLog(ctx context.Context) (eventlog.Eventlog, error)
}

type ReadPolicy interface {
	WritePolicy

	Offset() int64
	Forward(diff int)
}

type LogPolicy interface {
	AccessMode() policyType
}

var _ WritePolicy = (*roundRobinWritePolicy)(nil)

func NewRoundRobinWritePolicy(eb Eventbus) WritePolicy {
	return &roundRobinWritePolicy{
		bus: eb,
	}
}

type roundRobinWritePolicy struct {
	bus Eventbus
	idx uint64
}

func (w *roundRobinWritePolicy) Type() policyType {
	return RoundRobin
}

func (w *roundRobinWritePolicy) NextLog(ctx context.Context) (eventlog.Eventlog, error) {
	for {
		logs, err := w.bus.ListLog(ctx)
		if err != nil {
			return nil, err
		}
		if len(logs) == 0 {
			continue
		}
		l := len(logs)
		i := atomic.AddUint64(&w.idx, 1) % uint64(l)
		return logs[i], nil
	}
}

var _ ReadPolicy = (*roundRobinReadPolicy)(nil)

func NewRoundRobinReadPolicy(log eventlog.Eventlog, fromWhere consumeFromWhere) *roundRobinReadPolicy {
	var (
		off int64
		err error
	)
	if fromWhere == ConsumeFromWhereEarliest {
		off, err = log.EarliestOffset(context.Background())
		if err != nil {
			off = 0
		}
	} else if fromWhere == ConsumeFromWhereLatest {
		off, err = log.LatestOffset(context.Background())
		if err != nil {
			off = 0
		}
	}
	return &roundRobinReadPolicy{
		log:    log,
		offset: off,
	}
}

type roundRobinReadPolicy struct {
	log    eventlog.Eventlog
	offset int64
}

func (r roundRobinReadPolicy) Type() policyType {
	return RoundRobin
}

func (r roundRobinReadPolicy) NextLog(ctx context.Context) (eventlog.Eventlog, error) {
	return r.log, nil
}

func (r roundRobinReadPolicy) Offset() int64 {
	return atomic.LoadInt64(&r.offset)
}

func (r *roundRobinReadPolicy) Forward(diff int) {
	atomic.AddInt64(&r.offset, int64(diff))
}

var _ ReadPolicy = (*manuallyReadPolicy)(nil)

func NewManuallyReadPolicy(log eventlog.Eventlog, offset int64) *manuallyReadPolicy {
	return &manuallyReadPolicy{
		log:    log,
		offset: offset,
	}
}

type manuallyReadPolicy struct {
	log    eventlog.Eventlog
	offset int64
}

func (r manuallyReadPolicy) Type() policyType {
	return Manually
}

func (r manuallyReadPolicy) NextLog(ctx context.Context) (eventlog.Eventlog, error) {
	return r.log, nil
}

func (r manuallyReadPolicy) Offset() int64 {
	return atomic.LoadInt64(&r.offset)
}

func (r *manuallyReadPolicy) Forward(diff int) {
	atomic.AddInt64(&r.offset, int64(diff))
}

var _ LogPolicy = (*readOnlyPolicy)(nil)

func NewReadOnlyPolicy() LogPolicy {
	return &readOnlyPolicy{}
}

type readOnlyPolicy struct{}

func (w *readOnlyPolicy) AccessMode() policyType {
	return ReadOnly
}

var _ LogPolicy = (*readWritePolicy)(nil)

func NewReadWritePolicy() LogPolicy {
	return &readWritePolicy{}
}

type readWritePolicy struct{}

func (w *readWritePolicy) AccessMode() policyType {
	return ReadWrite
}
