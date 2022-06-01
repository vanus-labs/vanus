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

package store

import (
	// standard libraries.
	"sync"

	// this project.
	"github.com/linkall-labs/vanus/client/pkg/errors"
)

func NewAllocator() *Allocator {
	return &Allocator{
		stores: make(map[string]*BlockStore),
		mu:     sync.RWMutex{},
	}
}

type Allocator struct {
	stores map[string]*BlockStore
	mu     sync.RWMutex
}

// Get acquire BlockStore.
func (a *Allocator) Get(endpoint string) (*BlockStore, error) {
	if endpoint == "" {
		return nil, errors.ErrNoEndpoint
	}

	bs := func() *BlockStore {
		a.mu.RLock()
		defer a.mu.RUnlock()
		bs := a.stores[endpoint]
		if bs != nil {
			bs.Acquire()
		}
		return bs
	}()

	if bs == nil {
		a.mu.Lock()
		defer a.mu.Unlock()

		bs = a.stores[endpoint]
		if bs == nil { // double check
			var err error
			bs, err = newBlockStore(endpoint)
			if err != nil {
				return nil, err
			}
			a.stores[endpoint] = bs
		}
		bs.Acquire()
	}

	return bs, nil
}

// Put release BlockStore.
func (a *Allocator) Put(bs *BlockStore) {
	d := false
	if bs.Release() {
		func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if bs.UseCount() == 0 { // double check
				delete(a.stores, bs.Endpoint())
				d = true
			}
		}()
	}
	if d {
		bs.Close()
	}
}
