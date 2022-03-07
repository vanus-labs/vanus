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

package queue

import (
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"time"
)

type Queue interface {
	Add(key string)
	Len() int
	Get() (value string, shutdown bool)
	Done(key string)
	ShutDown()
	IsShutDown() bool
	ReAdd(key string)
	GetFailNum(key string) int
	ClearFailNum(key string)
}

type queue struct {
	queue workqueue.RateLimitingInterface
}

func New() Queue {
	return &queue{
		queue: workqueue.NewNamedRateLimitingQueue(DefaultControllerRateLimiter(), ""),
	}
}

func DefaultControllerRateLimiter() workqueue.RateLimiter {
	return workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 10*time.Second),
		// 10 qps, 100 bucket size.  This is only for retry speed and its only the overall factor (not per item)
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

func (q *queue) Add(key string) {
	q.queue.Add(key)
}

func (q *queue) Len() int {
	return q.queue.Len()
}

func (q *queue) Get() (value string, shutdown bool) {
	v, shutdown := q.queue.Get()
	if shutdown {
		return "", shutdown
	} else {
		return v.(string), shutdown
	}
}

func (q *queue) Done(key string) {
	q.queue.Done(key)
}

func (q *queue) ShutDown() {
	q.queue.ShutDown()
}

func (q *queue) IsShutDown() bool {
	return q.queue.ShuttingDown()
}

func (q *queue) ReAdd(key string) {
	q.queue.Done(key)
	q.queue.AddRateLimited(key)
}

func (q *queue) GetFailNum(key string) int {
	return q.queue.NumRequeues(key)
}

func (q *queue) ClearFailNum(key string) {
	q.queue.Forget(key)
}
