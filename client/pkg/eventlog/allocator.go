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

package eventlog

import (
	"context"
	"fmt"
	"github.com/linkall-labs/vanus/observability/tracing"
	"go.opentelemetry.io/otel/trace"
	"sync"
)

func NewAllocator() *Allocator {
	return &Allocator{
		eventlogs: make(map[string]EventLog),
		mu:        sync.RWMutex{},
		tracer:    tracing.NewTracer("pkg.eventlog.Allocator", trace.SpanKindClient),
	}
}

type Allocator struct {
	eventlogs map[string]EventLog
	mu        sync.RWMutex
	tracer    *tracing.Tracer
}

// Get acquire EventLog.
func (a *Allocator) Get(ctx context.Context, vrn string) (EventLog, error) {
	_, span := a.tracer.Start(ctx, "Get")
	defer span.End()

	cfg, err := ParseVRN(vrn)
	if err != nil {
		return nil, err
	}

	s := cfg.VRN.String()
	el := func() EventLog {
		a.mu.RLock()
		defer a.mu.RUnlock()
		el := a.eventlogs[s]
		if el != nil {
			el.Acquire()
		}
		return el
	}()

	if el == nil {
		a.mu.Lock()
		defer a.mu.Unlock()

		el = a.eventlogs[s]
		if el == nil { // double check
			el, err = a.doNew(cfg)
			if err != nil {
				return nil, err
			}
			a.eventlogs[s] = el
		}
		el.Acquire()
	}

	return el, nil
}

func (a *Allocator) doNew(cfg *Config) (EventLog, error) {
	newOp := Find(cfg.Scheme)
	if newOp == nil {
		panic(fmt.Sprintf("can not support scheme: %s", cfg.Kind))
	}
	return newOp(cfg)
}

// Put release EventLog.
func (a *Allocator) Put(ctx context.Context, el EventLog) {
	_ctx, span := a.tracer.Start(ctx, "Put")
	defer span.End()

	d := false
	if el.Release() {
		func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if el.UseCount() == 0 { // double check
				delete(a.eventlogs, el.VRN().String())
				d = true
			}
		}()
	}
	if d {
		el.Close(_ctx)
	}
}
