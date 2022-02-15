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

package trigger

import (
	"context"
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/ds"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

type TGState string

const (
	TGCreated   = "created"
	TGPending   = "pending"
	TGRunning   = "running"
	TGSleep     = "sleep"
	TGPaused    = "paused"
	TGStopped   = "stopped"
	TGDestroyed = "destroyed"
)

const (
	defaultBufferSize = 1 << 20
)

type Trigger struct {
	ID               string        `json:"id"`
	SubscriptionID   string        `json:"subscription_id"`
	BufferSize       int           `json:"buffer_size"`
	Target           primitive.URI `json:"target"`
	BatchProcessSize int           `json:"batch_process_size"`
	MaxRetryTimes    int           `json:"max_retry_times"`
	SleepDuration    time.Duration `json:"sleep_duration"`

	state      TGState
	stateMutex sync.RWMutex
	buffer     ds.RingBuffer
	exit       chan struct{}
	exitWG     sync.WaitGroup
	lastActive time.Time
	ackWindow  ds.SortedMap
}

func NewTrigger(sub *primitive.Subscription) *Trigger {
	return &Trigger{
		ID:               uuid.New().String(),
		SubscriptionID:   sub.ID,
		BufferSize:       defaultBufferSize,
		BatchProcessSize: 8,
		MaxRetryTimes:    3,
		SleepDuration:    30 * time.Second,
		state:            TGCreated,
		buffer:           ds.NewRingBuffer(defaultBufferSize),
		exit:             make(chan struct{}, 0),
		ackWindow:        ds.NewSortedMap(),
	}
}

func (t *Trigger) EventArrived(ctx context.Context, events ...*ce.Event) {
	err := t.buffer.BatchPut(events)
	for err != nil {
		err = t.buffer.BatchPut(events)
		log.Debug("buffer no left space", map[string]interface{}{
			"error": err,
		})
		time.Sleep(1 * time.Millisecond)
	}
}

func (t *Trigger) IsNeedFill() bool {
	return t.buffer.Capacity()-t.buffer.Length() > t.BufferSize
}

func (t *Trigger) Start(ctx context.Context) error {
	// running task
	go func() {
		retryTimes := 0
		t.exitWG.Add(1)
	LOOP:
		for {
			select {
			case _, ok := <-t.exit:
				if ok {
					log.Info("trigger exit", map[string]interface{}{
						"id": t.ID,
					})
				}
				break LOOP
			default:
				num, err := t.process(t.BatchProcessSize)
				if err != nil {
					log.Debug("process event error", map[string]interface{}{
						"error": err,
					})
					if retryTimes > t.MaxRetryTimes {
						events := make([]*ce.Event, num)
						data := t.buffer.BatchGet(num)
						for k := range t.buffer.BatchGet(num) {
							events[k] = data[k].(*ce.Event)
						}
						err := t.asDeadLetter(events...)
						if err != nil {
							ids := make([]string, 0)
							for idx := range events {
								ids = append(ids, events[idx].ID())
							}
							log.Error("send dead event to dead letter failed", map[string]interface{}{
								"error":  err,
								"events": ids,
							})
						}
						retryTimes = 0
						t.buffer.RemoveFromHead(num)
						break
					}
					retryTimes++
					time.Sleep(3 * time.Second) // TODO 优化
					break
				}
				if num == 0 {
					break
				}
				retryTimes = 0
				t.buffer.RemoveFromHead(num)
			}
		}
		t.exitWG.Add(-1)
	}()

	// sleep watch
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		t.exitWG.Add(1)
	LOOP:
		for {
			select {
			case _, ok := <-t.exit:
				if ok {
					log.Info("trigger sleep exit", map[string]interface{}{
						"id": t.ID,
					})
				}
				tk.Stop()
				break LOOP
			case _ = <-tk.C:
				t.stateMutex.Lock()
				if t.state == TGRunning {
					if time.Now().Sub(t.lastActive) > t.SleepDuration {
						t.state = TGSleep
					} else {
						t.state = TGRunning
					}
				}
				t.stateMutex.Unlock()
			}
		}
		t.exitWG.Add(-1)
	}()

	// monitor timeout
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		t.exitWG.Add(1)
	LOOP:
		for {
			select {
			case _, ok := <-t.exit:
				if ok {
					log.Info("trigger monitor ack exit", map[string]interface{}{
						"id": t.ID,
					})
				}
				tk.Stop()
				break LOOP
			case _ = <-tk.C:
				t.checkACKTimeout()
			}
		}
		t.exitWG.Add(-1)
	}()
	t.state = TGRunning
	t.lastActive = time.Now()
	return nil
}

func (t *Trigger) GetState() TGState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Trigger) Stop(ctx context.Context) error {
	close(t.exit)
	t.exitWG.Wait()
	_, err := t.process(t.buffer.Length())
	t.state = TGStopped
	return err
}

func (t *Trigger) process(num int) (int, error) {
	events := t.buffer.BatchGet(num)
	if len(events) == 0 {
		//log.Debug("no more event arrived", map[string]interface{}{
		//	"id":           t.ID,
		//	"subscription": t.SubscriptionID,
		//})
		time.Sleep(1 * time.Millisecond)
		return 0, nil
	}
	t.lastActive = time.Now()
	for idx := range events {
		events[idx] = events[idx]
		// TODO filtering & pushing
	}
	return len(events), nil
}

func (t *Trigger) asDeadLetter(events ...*ce.Event) error {
	fmt.Println(events)
	return nil
}

func (t *Trigger) ack(ids ...string) error {
	for _, v := range ids {
		t.ackWindow.Remove(v)
	}
	return nil
}

func (t *Trigger) checkACKTimeout() error {
	entry := t.ackWindow.Head()
	if entry == nil {
		return nil
	}
	wa := entry.Value().(*waitACK)
	for wa != nil && wa.timeout() {
		t.asDeadLetter(wa.event)
		t.ackWindow.Remove(entry.Key())
		entry = entry.Next()
		if entry != nil {
			wa = entry.Value().(*waitACK)
		} else {
			wa = nil
		}
	}
	return nil
}

type waitACK struct {
	event       *ce.Event
	deliverTime time.Time
	timeoutTime time.Time
}

func (wa *waitACK) timeout() bool {
	return time.Now().Sub(wa.timeoutTime) > 0
}
