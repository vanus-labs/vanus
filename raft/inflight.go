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

package raft

import "sort"

type waiter struct {
	index uint64
	cb    ProposeCallback
}

type inflight struct {
	waiters []waiter
}

func (in *inflight) append(index uint64, cb ProposeCallback) {
	in.waiters = append(in.waiters, waiter{
		index: index,
		cb:    cb,
	})
}

func (in *inflight) commitTo(index uint64) {
	size := len(in.waiters)
	if size == 0 {
		return
	}
	n := sort.Search(size, func(i int) bool {
		return in.waiters[i].index > index
	})
	committed := in.waiters[:n]
	in.waiters = in.waiters[n:]
	// TODO(james.yin): invoke callbacks in other goroutine
	for _, w := range committed {
		w.cb(nil)
	}
}

func (in *inflight) truncateFrom(index uint64) {
	size := len(in.waiters)
	if size == 0 {
		return
	}
	n := sort.Search(size, func(i int) bool {
		return in.waiters[i].index >= index
	})
	if n < size {
		dropped := append([]waiter{}, in.waiters[n:]...)
		in.waiters = in.waiters[:n]
		// TODO(james.yin): invoke callbacks in other goroutine
		for _, w := range dropped {
			w.cb(ErrProposalDropped)
		}
	}
}
