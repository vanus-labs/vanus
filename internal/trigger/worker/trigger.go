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

package worker

import (
	"context"
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/ds"
	"github.com/linkall-labs/vanus/internal/trigger/consumer"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

type TriggerState string

const (
	TriggerCreated   = "created"
	TriggerPending   = "pending"
	TriggerRunning   = "running"
	TriggerSleep     = "sleep"
	TriggerPaused    = "paused"
	TriggerStopped   = "stopped"
	TriggerDestroyed = "destroyed"
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

	state      TriggerState
	stateMutex sync.RWMutex
	exit       chan struct{}
	exitWG     sync.WaitGroup
	lastActive time.Time
	ackWindow  ds.SortedMap

	eventCh  chan *consumer.EventRecord
	ceClient ce.Client
	filter   filter.Filter
}

func NewTrigger(sub *primitive.Subscription) *Trigger {
	ceClient, err := primitive.NewCeClient(sub.Sink)
	if err != nil {
		log.Error("new ce-client error", map[string]interface{}{"target": sub.Sink, "error": err.Error()})
	}
	return &Trigger{
		ID:               uuid.New().String(),
		SubscriptionID:   sub.ID,
		Target:           sub.Sink,
		BufferSize:       defaultBufferSize,
		BatchProcessSize: 8,
		MaxRetryTimes:    3,
		SleepDuration:    30 * time.Second,
		state:            TriggerCreated,
		exit:             make(chan struct{}, 0),
		ackWindow:        ds.NewSortedMap(),
		filter:           filter.GetFilter(sub.Filters),
		ceClient:         ceClient,
		eventCh:          make(chan *consumer.EventRecord, defaultBufferSize),
	}
}

func (t *Trigger) EventArrived(ctx context.Context, event *consumer.EventRecord) error {
	select {
	case t.eventCh <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Trigger) processEvent(e ce.Event) error {
	if res := filter.FilterEvent(t.filter, e); res == filter.FailFilter {
		return nil
	}
	// TODO The ack here is represent http response 200, optimize to async
	if res := t.ceClient.Send(context.Background(), e); !ce.IsACK(res) {
		return res
	}
	return nil
}

func (t *Trigger) retryProcessEvent(e ce.Event) error {
	retryTimes := 0
	for {
		err := t.processEvent(e)
		if err == nil {
			return nil
		}
		retryTimes++
		if retryTimes >= t.MaxRetryTimes {
			return err
		}
		log.Info("process event error", map[string]interface{}{
			"error": err, "retryTimes": retryTimes,
		})
		time.Sleep(3 * time.Second) //TODO 优化
	}
}

func (t *Trigger) startProcessEvent(ctx context.Context) {
	t.exitWG.Add(1)
	defer t.exitWG.Done()
	for event := range t.eventCh {
		log.Debug("receive a event", map[string]interface{}{"event": event})
		t.lastActive = time.Now()
		err := t.retryProcessEvent(*event.Event)
		if err == nil {
			consumer.GetEventLogOffset(t.SubscriptionID).EventCommit(event)
			continue
		}
		log.Warning("send event fail,will put to dead letter", map[string]interface{}{
			"error": err,
			"event": event,
		})
		err = t.asDeadLetter(event)
		if err != nil {
			log.Error("send dead event to dead letter failed", map[string]interface{}{
				"error": err,
				"event": event,
			})
		} else {
			consumer.GetEventLogOffset(t.SubscriptionID).EventCommit(event)
		}
	}
}
func (t *Trigger) Start(ctx context.Context) error {
	go t.startProcessEvent(ctx)
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
				if t.state == TriggerRunning {
					if time.Now().Sub(t.lastActive) > t.SleepDuration {
						t.state = TriggerSleep
					} else {
						t.state = TriggerRunning
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
	t.state = TriggerRunning
	t.lastActive = time.Now()
	return nil
}

func (t *Trigger) GetState() TriggerState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Trigger) Stop(ctx context.Context) {
	if t.state == TriggerStopped {
		return
	}
	close(t.exit)
	close(t.eventCh)
	t.exitWG.Wait()
	t.state = TriggerStopped
}

func (t *Trigger) asDeadLetter(events ...*consumer.EventRecord) error {
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
		// TODO how to deal with timeout event?
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
	event       *consumer.EventRecord
	deliverTime time.Time
	timeoutTime time.Time
}

func (wa *waitACK) timeout() bool {
	return time.Now().Sub(wa.timeoutTime) > 0
}
