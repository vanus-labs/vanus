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

package sync

import (
	_ "unsafe" // for go:linkname
)

type Semaphore struct {
	sem     uint32
	handoff bool
}

func (s *Semaphore) Init(handoff bool) *Semaphore {
	s.handoff = handoff
	return s
}

func (s *Semaphore) Acquire() {
	semacquire(&s.sem)
}

func (s *Semaphore) Release() {
	semrelease(&s.sem, s.handoff, 0)
}

//go:linkname semacquire sync.runtime_Semacquire
func semacquire(addr *uint32)

//go:linkname semrelease sync.runtime_Semrelease
func semrelease(addr *uint32, handoff bool, skipframes int)

/*
import (
	// standard libraries.
	stdrt "runtime"
	"sync/atomic"
	"unsafe"

	// third-party libraries.
	"gvisor.dev/gvisor/pkg/sync"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/container/conque/unbounded"
	"github.com/vanus-labs/vanus/internal/primitive/runtime"
)

const (
	running  uintptr = 0
	runnable uintptr = 1

	enableReloadPtr1 = false

	activeSpin    = 4
	activeSpinCnt = 30
	passiveSpin   = 1
)

var ncpu = stdrt.NumCPU()

type waiter struct {
	// key is the status of waiter's goroutine. The value of key is running, runnable or gp (treats it as waiting).
	key uintptr
}

type Semaphore struct {
	sem int64
	q   unbounded.Queue[*waiter]
}

func (s *Semaphore) Acquire() {
	if atomic.AddInt64(&s.sem, -1) >= 0 {
		return
	}

	w := &waiter{}
	s.q.Push(w)

	sync.Gopark(semaphoreCommit, unsafe.Pointer(w), sync.WaitReasonSemacquire, sync.TraceEvGoBlockSync, 0)
}

//go:norace
func semaphoreCommit(gp uintptr, wp unsafe.Pointer) bool {
	w := (*waiter)(wp)
	// return atomic.CompareAndSwapUintptr(&w.key, running, gp)
	return atomic.SwapUintptr(&w.key, gp) != runnable
}

func (s *Semaphore) Release() {
	if atomic.AddInt64(&s.sem, 1) > 0 {
		return
	}

	w := getWaiter(&s.q)

	key := atomic.SwapUintptr(&w.key, runnable)
	if key != running {
		sync.Goready(key, 0, true)
	}
}

func getWaiter(q *unbounded.Queue[*waiter]) *waiter {
	if enableReloadPtr1 && ncpu <= 1 {
		return getWaiter1(q)
	}
	return getWaiterN(q)
}

func getWaiter1(q *unbounded.Queue[*waiter]) *waiter {
	for i := 0; ; i++ {
		w, ok := q.SharedPop()
		if ok {
			return w
		}

		switch {
		case i < passiveSpin:
			runtime.OSYield()
		default:
			// TODO(james.yin): use condition variable?
			runtime.OSYield()
		}
	}
}

func getWaiterN(q *unbounded.Queue[*waiter]) *waiter {
	for i := 0; ; i++ {
		w, ok := q.SharedPop()
		if ok {
			return w
		}

		switch {
		case i < activeSpin:
			runtime.ProcYield(activeSpinCnt)
		case i < activeSpin+passiveSpin:
			runtime.OSYield()
		default:
			// TODO(james.yin): use condition variable?
			runtime.OSYield()
		}
	}
}
*/
