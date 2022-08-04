// Copyright 2022 Linkall Inc.
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

package segment

import (
	"context"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"sync"
)

var _ = pollingManager(&pm{})

type pollingManager interface {
	Add(ctx context.Context, blockID vanus.ID) <-chan struct{}
	NewMessageArrived(blockID vanus.ID)
	Destroy()
}

type pm struct {
	// vanus.ID, *blockPolling
	blockPollingMap sync.Map
}

func (p *pm) Destroy() {
	p.blockPollingMap.Range(func(_, value interface{}) bool {
		value.(*blockPolling).destroy()
		return true
	})
}

func (p *pm) Add(ctx context.Context, blockID vanus.ID) <-chan struct{} {
	v, exist := p.blockPollingMap.Load(blockID)
	if !exist {
		bp := newBlockPolling()
		actual, loaded := p.blockPollingMap.LoadOrStore(blockID, newBlockPolling())
		if loaded {
			bp.destroy()
		}
		v = actual
	}
	bp := v.(*blockPolling)
	return bp.add(ctx)
}

func (p *pm) NewMessageArrived(blockID vanus.ID) {
	v, exist := p.blockPollingMap.Load(blockID)
	if !exist {
		return
	}
	v.(*blockPolling).messageArrived()
}

type blockPolling struct {
	mutex sync.Mutex
	ch    chan struct{}
}

func newBlockPolling() *blockPolling {
	bp := &blockPolling{
		mutex: sync.Mutex{},
		ch:    make(chan struct{}),
	}
	return bp
}

func (bp *blockPolling) add(ctx context.Context) <-chan struct{} {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	_, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	return bp.ch
}

func (bp *blockPolling) messageArrived() {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	close(bp.ch)
	bp.ch = make(chan struct{})
}

func (bp *blockPolling) destroy() {
	close(bp.ch)
}
