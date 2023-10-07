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

package executor

import (
	// standard libraries.
	"sync/atomic"

	// this project.
	"github.com/vanus-labs/vanus/lib/container/conque/blocking"
	"github.com/vanus-labs/vanus/lib/container/conque/unbounded"
)

const defaultInvokeBatchSize = 8

type flow struct {
	q     unbounded.Queue[Task]
	state int32
	mf    *MultiFlow
}

// Make sure flow implements ExecuteCloser.
var _ ExecuteCloser = (*flow)(nil)

func (f *flow) Execute(t Task) bool {
	if atomic.LoadInt32(&f.state) != 0 {
		return false
	}

	if f.q.Push(t) {
		f.mf.q.Push(f)
	}
	return true
}

func (f *flow) Close() {
	atomic.StoreInt32(&f.state, 1)
}

func (f *flow) invokeTasks(batch int) bool {
	if atomic.LoadInt32(&f.state) != 0 {
		return false
	}

	for i := 0; i < batch; i++ {
		t, _ := f.q.Peek()

		t()

		_, empty, _ := f.q.UniquePop()
		if empty {
			return false
		}
	}
	return true
}

type MultiFlow struct {
	q        blocking.Queue[*flow]
	parallel int
}

func NewMultiFlow(parallel int, handoff, startImmediately bool) *MultiFlow {
	return new(MultiFlow).Init(parallel, handoff, startImmediately)
}

func (mf *MultiFlow) Init(parallel int, handoff, startImmediately bool) *MultiFlow {
	mf.parallel = parallel
	mf.q.Init(handoff)
	if startImmediately {
		mf.Start()
	}
	return mf
}

func (mf *MultiFlow) Start() {
	for i := 0; i < mf.parallel; i++ {
		go mf.run()
	}
}

func (mf *MultiFlow) Close() {
	mf.q.Close()
}

func (mf *MultiFlow) NewFlow() ExecuteCloser {
	f := &flow{
		mf: mf,
	}
	return f
}

func (mf *MultiFlow) run() {
	for {
		f, ok := mf.q.SharedPop()
		if !ok {
			return
		}

		if f.invokeTasks(defaultInvokeBatchSize) {
			mf.q.Push(f)
		}
	}
}
