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

package unbounded

import (
	// standard libraries.
	stdrt "runtime"
	"sync/atomic"
	"unsafe"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/runtime"
)

const (
	enableReloadPtr1 = false

	activeSpin    = 4
	activeSpinCnt = 30
	passiveSpin   = 1
)

var (
	ncpu = stdrt.NumCPU()
	lock = unsafe.Pointer(&struct{}{})
)

type node[T any] struct {
	_    [7]uint64 // padding to fill cache line
	next unsafe.Pointer
	v    T
}

// Queue is a concurrency safety non-blocking queue,
// used in scenarios with multiple Producers and single Consumers.
type Queue[T any] struct {
	_    [7]uint64 // padding to fill cache line
	tail unsafe.Pointer
	_    [7]uint64 // padding to fill cache line
	head unsafe.Pointer
	_    [7]uint64 // padding to fill cache line
}

func New[T any]() *Queue[T] {
	return new(Queue[T])
}

func (q *Queue[T]) Push(v T) bool {
	n := &node[T]{
		v: v,
	}
	return q.push(n)
}

func (q *Queue[T]) push(n *node[T]) bool {
	// n.next = nil
	nn := unsafe.Pointer(n)

	prev := atomic.SwapPointer(&q.tail, nn)
	if prev == nil {
		atomic.StorePointer(&q.head, nn)
		return true
	}

	atomic.StorePointer(&(*node[T])(prev).next, nn)
	return false
}

func (q *Queue[T]) SharedPop() (T, bool) {
	n, ok := q.sharedPop()
	if !ok {
		var v T
		return v, false
	}
	return n.v, true
}

func (q *Queue[T]) sharedPop() (*node[T], bool) {
	for {
		head := atomic.LoadPointer(&q.head)

		if head == lock {
			head = waitUnlock(&q.head)
		}

		// No node.
		if head == nil {
			return nil, false
		}

		next := atomic.LoadPointer(&(*node[T])(head).next)
		if next != nil {
			if atomic.CompareAndSwapPointer(&q.head, head, next) {
				return (*node[T])(head), true
			}
			continue
		}

		// Only one element, lock.
		if !atomic.CompareAndSwapPointer(&q.head, head, lock) {
			continue
		}

		if atomic.CompareAndSwapPointer(&q.tail, head, nil) {
			atomic.CompareAndSwapPointer(&q.head, lock, nil)
			return (*node[T])(head), true
		}

		// Push-pop conflict, spin.
		next = waitStable(&(*node[T])(head).next)

		atomic.StorePointer(&q.head, next)
		return (*node[T])(head), true
	}
}

func (q *Queue[T]) UniquePop() (T, bool, bool) {
	n, empty, ok := q.uniquePop()
	if !ok {
		var v T
		return v, empty, false
	}
	return n.v, empty, true
}

func (q *Queue[T]) uniquePop() (*node[T], bool, bool) {
	head := atomic.LoadPointer(&q.head)

	// No node.
	if head == nil {
		return nil, true, false
	}

	next := atomic.LoadPointer(&(*node[T])(head).next)

	// Only one element.
	if next == nil {
		if atomic.CompareAndSwapPointer(&q.tail, head, nil) {
			atomic.CompareAndSwapPointer(&q.head, head, nil)
			return (*node[T])(head), true, true
		}

		// Push-pop conflict, spin.
		next = waitStable(&(*node[T])(head).next)
	}

	atomic.StorePointer(&q.head, next)
	return (*node[T])(head), false, true
}

func (q *Queue[T]) Peek() (T, bool) {
	n, ok := q.peek()
	if !ok {
		var v T
		return v, false
	}
	return n.v, true
}

func (q *Queue[T]) peek() (*node[T], bool) {
	head := atomic.LoadPointer(&q.head)
	return (*node[T])(head), head != nil
}

func waitUnlock(addr *unsafe.Pointer) unsafe.Pointer {
	return reloadPtr(addr, lock)
}

func waitStable(addr *unsafe.Pointer) unsafe.Pointer {
	return reloadPtr(addr, nil)
}

func reloadPtr(addr *unsafe.Pointer, unexpected unsafe.Pointer) unsafe.Pointer {
	if enableReloadPtr1 && ncpu <= 1 {
		return reloadPtr1(addr, unexpected)
	}
	return reloadPtrN(addr, unexpected)
}

func reloadPtr1(addr *unsafe.Pointer, unexpected unsafe.Pointer) unsafe.Pointer {
	for i := 0; ; i++ {
		p := atomic.LoadPointer(addr)
		if p != unexpected {
			return p
		}

		switch {
		case i < passiveSpin:
			runtime.OSYield()
		default:
			// TODO(james.yin): use synchronization primitive?
			stdrt.Gosched()
		}
	}
}

func reloadPtrN(addr *unsafe.Pointer, unexpected unsafe.Pointer) unsafe.Pointer {
	for i := 0; ; i++ {
		p := atomic.LoadPointer(addr)
		if p == nil {
			_ = 1
		}
		if p != unexpected {
			return p
		}

		switch {
		case i < activeSpin:
			runtime.ProcYield(activeSpinCnt)
		case i < activeSpin+passiveSpin:
			runtime.OSYield()
		default:
			// TODO(james.yin): use synchronization primitive?
			stdrt.Gosched()
		}
	}
}
