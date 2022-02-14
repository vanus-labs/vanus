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
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/ds"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)
import ce "github.com/cloudevents/sdk-go/v2"

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
	lastActive time.Time
	//TODO ACK Management
}

func NewTrigger(sub primitive.Subscription) *Trigger {
	return &Trigger{
		ID: uuid.New().String(),
	}
}

func (t *Trigger) EventArrived(ctx context.Context, events ...*ce.Event) {
	t.buffer.BatchPut(events)
}

func (t *Trigger) IsNeedFill() bool {
	return !t.buffer.IsFull() // TODO remain size < batch size
}

func (t *Trigger) Start(ctx context.Context) error {
	// running task
	go func() {
		retryTimes := 0
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
					if retryTimes > t.MaxRetryTimes {
						// TODO dead letter
						events := make([]*ce.Event, num)
						data := t.buffer.BatchGet(num)
						for k := range t.buffer.BatchGet(num) {
							events[k] = data[k].(*ce.Event)
						}
						t.sendToDeadLetter(events...)
						retryTimes = 0
						t.buffer.RemoveFromHead(num)
						break
					}
					retryTimes++
					time.Sleep(3 * time.Second) // TODO 优化
					break
				}
				retryTimes = 0
				t.buffer.RemoveFromHead(num)
			}
		}
	}()

	// sleep watch
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
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
						t.state = TGRunning
					} else {
						t.state = TGRunning
					}
				}
				t.stateMutex.Unlock()
			}
		}
	}()
	return nil
}

func (t *Trigger) GetState() TGState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Trigger) Stop(ctx context.Context) error {
	close(t.exit)
	_, err := t.process(t.buffer.Length())
	return err
}

func (t *Trigger) process(num int) (int, error) {
	events := t.buffer.BatchGet(num)
	if len(events) == 0 {
		time.Sleep(10 * time.Millisecond)
		return 0, nil
	}
	t.lastActive = time.Now()
	// TODO filtering & pushing
	return len(events), nil
}

func (t *Trigger) sendToDeadLetter(events ...*ce.Event) error {
	return nil
}
