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
	"sync"
	"time"
)

type PendingTask interface {
	OnTimeout(pid PendingID)
}

type PendingID interface{}

type pendingNode struct {
	task     PendingTask
	deadline time.Time
	prev     *pendingNode
	next     *pendingNode
}

func (pn *pendingNode) onTimeout() {
	pn.task.OnTimeout(pn)
}

type pendingQueue struct {
	mu    sync.Mutex
	tasks pendingNode
	delay time.Duration
}

func newPendingQueue(delay time.Duration) *pendingQueue {
	pq := &pendingQueue{
		delay: delay,
	}
	pq.tasks.prev = &pq.tasks
	pq.tasks.next = &pq.tasks

	go pq.run()

	return pq
}

func (pq *pendingQueue) Close() {
	// TODO(james.yin): close
}

func (pq *pendingQueue) run() {
	// TODO(james.yin): optimize
	pq.mu.Lock()
	for {
		now := time.Now()
		node := pq.tasks.next
		if node == &pq.tasks {
			pq.mu.Unlock()
			time.Sleep(pq.delay)
			pq.mu.Lock()
			continue
		}
		if now.Before(node.deadline) {
			pq.mu.Unlock()
			time.Sleep(node.deadline.Sub(now))
			pq.mu.Lock()
			continue
		}
		removeNode(node)

		pq.mu.Unlock()
		node.onTimeout()
		pq.mu.Lock()
	}
}

func (pq *pendingQueue) Push(t PendingTask) PendingID {
	// TODO(james.yin): optimize lock
	pq.mu.Lock()
	defer pq.mu.Unlock()
	node := &pendingNode{
		task:     t,
		deadline: time.Now().Add(pq.delay),
		prev:     pq.tasks.prev,
		next:     &pq.tasks,
	}
	pq.tasks.prev.next = node
	pq.tasks.prev = node
	return node
}

func (pq *pendingQueue) Cancel(pid PendingID) {
	node, _ := pid.(*pendingNode)
	// TODO(james.yin): optimize lock
	pq.mu.Lock()
	defer pq.mu.Unlock()
	removeNode(node)
}

func removeNode(node *pendingNode) {
	if node.next == node {
		return
	}
	node.next.prev = node.prev
	node.prev.next = node.next
	node.next = node
	node.prev = node
}
