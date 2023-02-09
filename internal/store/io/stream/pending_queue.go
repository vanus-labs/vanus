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

package stream

import (
	// standard library.
	"sync/atomic"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/container/conque/blocking"
)

type PendingTask interface {
	OnTimeout(pid PendingID)
}

type PendingID interface{}

type pendingNode struct {
	task     PendingTask
	deadline time.Time
	state    int32
}

func (pn *pendingNode) onTimeout() {
	pn.task.OnTimeout(pn)
}

type pendingQueue struct {
	q     blocking.Queue[*pendingNode]
	delay time.Duration
}

func (pq *pendingQueue) init(delay time.Duration) *pendingQueue {
	// NOTE: delay is short.
	pq.delay = delay

	go pq.run()

	return pq
}

func (pq *pendingQueue) Close() {
	pq.q.Close()
}

func (pq *pendingQueue) run() {
	now := time.Now()
	for {
		node, ok := pq.q.UniquePop()
		if !ok {
			return
		}

		// Shortcut if task is canceled.
		if atomic.LoadInt32(&node.state) != 0 {
			continue
		}

		if now.Before(node.deadline) {
			// refresh time
			now = time.Now()
			if now.Before(node.deadline) {
				time.Sleep(node.deadline.Sub(now))

				// refresh time
				now = time.Now()

				// recheck node state
				if atomic.LoadInt32(&node.state) != 0 {
					continue
				}
			}
		}

		node.onTimeout()
	}
}

func (pq *pendingQueue) Push(t PendingTask) PendingID {
	node := &pendingNode{
		task:     t,
		deadline: time.Now().Add(pq.delay),
	}
	pq.q.Push(node)
	return node
}

func (pq *pendingQueue) Cancel(pid PendingID) {
	node, _ := pid.(*pendingNode)
	atomic.StoreInt32(&node.state, 1)
}
