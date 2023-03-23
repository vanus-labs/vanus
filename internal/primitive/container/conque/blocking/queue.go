// Copyright 2023 Linkall Inc.
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

package blocking

import (
	// standard libraries.
	stdsync "sync"
	"sync/atomic"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/container/conque/unbounded"
	"github.com/vanus-labs/vanus/internal/primitive/sync"
)

type Queue[T any] struct {
	q     unbounded.Queue[T]
	sem   sync.Semaphore
	mu    stdsync.RWMutex
	state int32
}

func New[T any](_ bool) *Queue[T] {
	return new(Queue[T])
}

func (q *Queue[T]) Init(handoff bool) *Queue[T] {
	q.sem.Init(handoff)
	return q
}

func (q *Queue[T]) Close() {
	atomic.StoreInt32(&q.state, 1)
	q.sem.Release()
}

// Wait esures that all incoming Pushes observe that the queue is closed.
func (q *Queue[T]) Wait() {
	// Make sure no inflight Push.
	q.mu.Lock()

	// no op
	_ = 1

	q.mu.Unlock()
}

func (q *Queue[T]) Push(v T) bool {
	// NOTE: no panic, avoid unlocking with defer.
	q.mu.RLock()

	// TODO: maybe atomic is unnecessary.
	if atomic.LoadInt32(&q.state) != 0 {
		q.mu.RUnlock()
		return false
	}

	_ = q.q.Push(v)
	q.sem.Release()
	q.mu.RUnlock()
	return true
}

func (q *Queue[T]) SharedPop() (T, bool) {
	q.sem.Acquire()

	// Check close.
	if atomic.LoadInt32(&q.state) != 0 {
		q.sem.Release()
		var v T
		return v, false
	}

	for {
		v, ok := q.q.SharedPop()
		if ok {
			return v, true
		}
	}
}

func (q *Queue[T]) UniquePop() (T, bool) {
	q.sem.Acquire()

	// Check close.
	if atomic.LoadInt32(&q.state) != 0 {
		q.sem.Release()
		var v T
		return v, false
	}

	for {
		v, _, ok := q.q.UniquePop()
		if ok {
			return v, true
		}
	}
}

func (q *Queue[T]) RawPop() (T, bool) {
	v, _, ok := q.q.UniquePop()
	return v, ok
}
