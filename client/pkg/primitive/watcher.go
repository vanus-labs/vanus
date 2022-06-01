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

package primitive

import (
	"context"
	"sync"
	"time"
)

func NewWatcher(period time.Duration, lookupFunc func(), cleanFuncs ...func()) *Watcher {
	w := &Watcher{
		period:     period,
		lookupFunc: lookupFunc,
		cleanFuncs: cleanFuncs,
		ch:         make(chan interface{}, 1), // TODO: no buffer
		wg:         nil,
		mu:         sync.RWMutex{},
	}
	return w
}

type Watcher struct {
	period     time.Duration
	lookupFunc func()
	cleanFuncs []func()

	ch chan interface{}
	wg *sync.WaitGroup
	mu sync.RWMutex
}

func (w *Watcher) Close() {
	close(w.ch)
}

func (w *Watcher) Run() {
	defer func() {
		for _, closeFunc := range w.cleanFuncs {
			closeFunc()
		}
	}()

	// do first lookup immediately
	t := time.NewTimer(0)
	for {
		isTimeout := false
		select {
		case _, ok := <-w.ch:
			if !ok {
				return
			}
		case <-t.C:
			isTimeout = true
		}

		w.lookupFunc()

		// reset timer
		if !t.Stop() && !isTimeout {
			<-t.C
		}
		t.Reset(w.period)
	}
}

func (w *Watcher) Refresh(ctx context.Context) error {
	// batch multi-refresh into a group

	wg := func() *sync.WaitGroup {
		w.mu.RLock()
		defer w.mu.RUnlock()
		return w.wg
	}()

	isLeader := false
	if wg == nil {
		wg = func() *sync.WaitGroup {
			w.mu.Lock()
			defer w.mu.Unlock()

			if w.wg == nil { // double check
				w.wg = &sync.WaitGroup{}
				w.wg.Add(1)
				isLeader = true
			}

			return w.wg
		}()
	}

	if isLeader {
		// TODO: non-blocking
		w.ch <- nil
	}

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		wg.Wait()
	}()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) Wakeup() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.wg != nil {
		w.wg.Done()
		// clear
		w.wg = nil
	}
}
