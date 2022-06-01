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

package inmemory

import (
	"context"
	"sync"

	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

// func init() {
// 	UseNameService("vanus+local")
// }

func UseNameService(scheme string) *NameService {
	ns := NewNameService()
	discovery.Register(scheme, ns)
	return ns
}

func NewNameService() *NameService {
	return &NameService{
		buses: make(map[uint64]*record.EventBus),
		mu:    sync.RWMutex{},
	}
}

type NameService struct {
	buses map[uint64]*record.EventBus
	mu    sync.RWMutex
}

// make sure NameService implements discovery.NameService.
var _ discovery.NameService = (*NameService)(nil)

func (ns *NameService) Lookup(eventbus *discovery.VRN) (*record.EventBus, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	bus := ns.buses[eventbus.ID]
	if bus != nil {
		return bus, nil
	}
	return nil, errors.ErrNotFound
}

func (ns *NameService) LookupWritableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	bus, err := ns.Lookup(eventbus)
	if err != nil {
		return nil, err
	}
	return bus.WritableLog(), nil
}

func (ns *NameService) LookupReadableLogs(ctx context.Context, eventbus *discovery.VRN) ([]*record.EventLog, error) {
	bus, err := ns.Lookup(eventbus)
	if err != nil {
		return nil, err
	}
	return bus.ReadableLog(), nil
}

func (ns *NameService) Register(eventbus *discovery.VRN, bus *record.EventBus) {
	if eventbus != nil && bus != nil {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		ns.buses[eventbus.ID] = bus
	}
}

func (ns *NameService) Unregister(eventbus *discovery.VRN) {
	if eventbus != nil {
		ns.mu.Lock()
		defer ns.mu.Unlock()
		delete(ns.buses, eventbus.ID)
	}
}
