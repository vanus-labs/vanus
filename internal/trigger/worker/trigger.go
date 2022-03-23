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
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
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
	defaultBufferSize = 1 << 10
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
	lastActive time.Time
	ackWindow  ds.SortedMap

	offsetManager *offset.SubscriptionOffset
	stop          context.CancelFunc
	eventCh       chan *info.EventRecord
	sendCh        chan *info.EventRecord
	deadLetterCh  chan *info.EventRecord
	ceClient      ce.Client
	filter        filter.Filter

	wg util.Group
}

func NewTrigger(sub *primitive.Subscription, offsetManager *offset.SubscriptionOffset) *Trigger {
	return &Trigger{
		ID:               uuid.New().String(),
		SubscriptionID:   sub.ID,
		Target:           sub.Sink,
		BufferSize:       defaultBufferSize,
		BatchProcessSize: 2,
		MaxRetryTimes:    3,
		SleepDuration:    30 * time.Second,
		state:            TriggerCreated,
		ackWindow:        ds.NewSortedMap(),
		filter:           filter.GetFilter(sub.Filters),
		eventCh:          make(chan *info.EventRecord, defaultBufferSize),
		sendCh:           make(chan *info.EventRecord, 20),
		deadLetterCh:     make(chan *info.EventRecord, defaultBufferSize),
		offsetManager:    offsetManager,
	}
}

func (t *Trigger) GetEventCh() chan *info.EventRecord {
	return t.eventCh
}

func (t *Trigger) EventArrived(ctx context.Context, event *info.EventRecord) error {
	select {
	case t.eventCh <- event:
		t.offsetManager.EventReceive(event.OffsetInfo)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *Trigger) retrySendEvent(ctx context.Context, e *ce.Event) error {
	retryTimes := 0
	doFunc := func() error {
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return t.ceClient.Send(timeout, *e)
	}
	var err error
	for retryTimes < t.MaxRetryTimes {
		retryTimes++
		if err = doFunc(); !ce.IsACK(err) {
			log.Debug(ctx, "process event error", map[string]interface{}{
				"error": err, "retryTimes": retryTimes,
			})
			time.Sleep(3 * time.Second) //TODO 优化
		} else {
			log.Debug(ctx, "send ce event success", map[string]interface{}{
				"event": e,
			})
			return nil
		}
	}
	return err
}

func (t *Trigger) runEventProcess(ctx context.Context) {
	for {
		select {
		//TODO  是否立即停止，还是等待eventCh处理完
		case <-ctx.Done():
			return
		case event, ok := <-t.eventCh:
			if !ok {
				return
			}
			if res := filter.FilterEvent(t.filter, *event.Event); res == filter.FailFilter {
				t.offsetManager.EventCommit(event.OffsetInfo)
				continue
			}
			t.sendCh <- event
		}
	}
}

func (t *Trigger) runEventSend(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.sendCh:
			if !ok {
				return
			}
			err := t.retrySendEvent(ctx, event.Event)
			if err != nil {
				t.deadLetterCh <- event
			} else {
				t.offsetManager.EventCommit(event.OffsetInfo)
			}
		}
	}
}

func (t *Trigger) runDeadLetterProcess(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.deadLetterCh:
			if !ok {
				return
			}
			err := t.asDeadLetter(event)
			if err != nil {
				log.Error(ctx, "send dead event to dead letter failed", map[string]interface{}{
					"error": err,
					"event": event,
				})
			}
			t.offsetManager.EventCommit(event.OffsetInfo)
		}
	}
}

func (t *Trigger) runSleepWatch(ctx context.Context) {
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
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
}
func (t *Trigger) runMonitorACK(ctx context.Context) {
	tk := time.NewTicker(10 * time.Millisecond)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.checkACKTimeout()
		}
	}
}

func (t *Trigger) Start(parent context.Context) error {
	ceClient, err := primitive.NewCeClient(t.Target)
	if err != nil {
		return errors.Wrapf(err, "new ce-client %s has error", t.Target)
	}
	t.ceClient = ceClient
	ctx, cancel := context.WithCancel(parent)
	t.stop = cancel
	for i := 0; i < t.BatchProcessSize; i++ {
		t.wg.StartWithContext(ctx, t.runEventProcess)
		t.wg.StartWithContext(ctx, t.runEventSend)
		t.wg.StartWithContext(ctx, t.runDeadLetterProcess)
	}
	t.wg.StartWithContext(ctx, t.runSleepWatch)
	t.wg.StartWithContext(ctx, t.runMonitorACK)

	t.state = TriggerRunning
	t.lastActive = time.Now()
	return nil
}

func (t *Trigger) Stop() {
	ctx := context.Background()
	log.Info(ctx, "trigger stop...", map[string]interface{}{
		"subId": t.SubscriptionID,
	})
	if t.state == TriggerStopped {
		return
	}
	t.stop()
	t.wg.Wait()

	t.state = TriggerStopped
	log.Info(ctx, "trigger stopped", map[string]interface{}{
		"subId": t.SubscriptionID,
	})
}

func (t *Trigger) GetState() TriggerState {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Trigger) asDeadLetter(events ...*info.EventRecord) error {
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
	event       *info.EventRecord
	deliverTime time.Time
	timeoutTime time.Time
}

func (wa *waitACK) timeout() bool {
	return time.Now().Sub(wa.timeoutTime) > 0
}
