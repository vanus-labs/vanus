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
	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/container/conque/unbounded"
	"github.com/linkall-labs/vanus/internal/primitive/sync"
)

type Queue[T any] struct {
	q   unbounded.Queue[T]
	sem sync.Semaphore
}

func New[T any](handoff bool) *Queue[T] {
	return new(Queue[T])
}

func (q *Queue[T]) Init(handoff bool) *Queue[T] {
	q.sem.Init(handoff)
	return q
}

func (q *Queue[T]) Close() {
	// FIXME(james.yin): implements this.
}

func (q *Queue[T]) Push(v T) {
	_ = q.q.Push(v)
	q.sem.Release()
}

func (q *Queue[T]) SharedPop() (T, bool) {
	q.sem.Acquire()

	// FIXME(james.yin): check close.

	for {
		v, ok := q.q.SharedPop()
		if ok {
			return v, true
		}
	}
}

func (q *Queue[T]) UniquePop() (T, bool) {
	q.sem.Acquire()

	// FIXME(james.yin): check close.

	for {
		v, _, ok := q.q.UniquePop()
		if ok {
			return v, true
		}
	}
}
