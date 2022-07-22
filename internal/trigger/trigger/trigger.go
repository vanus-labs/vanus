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
	"sync"
	"time"

	"go.uber.org/ratelimit"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/transformation"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
)

type State string

const (
	TriggerCreated   = "created"
	TriggerPending   = "pending"
	TriggerRunning   = "running"
	TriggerSleep     = "sleep"
	TriggerPaused    = "paused"
	TriggerStopped   = "stopped"
	TriggerDestroyed = "destroyed"

	sleepCheckPeriod = 10 * time.Millisecond
	sleepDuration    = 30 * time.Second
)

type Trigger struct {
	ID             vanus.ID      `json:"id"`
	SubscriptionID vanus.ID      `json:"subscription_id"`
	Target         primitive.URI `json:"target"`
	SleepDuration  time.Duration `json:"sleep_duration"`

	state      State
	stateMutex sync.RWMutex
	lastActive time.Time

	offsetManager *offset.SubscriptionOffset
	stop          context.CancelFunc
	eventCh       chan info.EventRecord
	sendCh        chan info.EventRecord
	ceClient      ce.Client
	filter        filter.Filter
	rateLimiter   ratelimit.Limiter

	inputTransformer *transformation.InputTransformer
	config           Config
	lock             sync.RWMutex

	wg util.Group
}

func NewTrigger(sub *primitive.Subscription, offsetManager *offset.SubscriptionOffset, opts ...Option) *Trigger {
	t := &Trigger{
		stop:           func() {},
		config:         defaultConfig(),
		ID:             vanus.NewID(),
		SubscriptionID: sub.ID,
		Target:         sub.Sink,
		SleepDuration:  sleepDuration,
		state:          TriggerCreated,
		filter:         filter.GetFilter(sub.Filters),
		offsetManager:  offsetManager,
	}
	t.applyOptions(opts...)
	t.eventCh = make(chan info.EventRecord, t.config.BufferSize)
	t.sendCh = make(chan info.EventRecord, t.config.BufferSize)
	if t.rateLimiter == nil {
		t.rateLimiter = ratelimit.NewUnlimited()
	}
	if sub.InputTransformer != nil {
		t.inputTransformer = transformation.NewInputTransformer(sub.InputTransformer)
	}
	return t
}

func (t *Trigger) applyOptions(opts ...Option) {
	for _, fn := range opts {
		fn(t)
	}
}

func (t *Trigger) getCeClient() ce.Client {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.ceClient
}

func (t *Trigger) ChangeTarget(target primitive.URI) error {
	ceClient, err := NewCeClient(t.Target)
	if err != nil {
		return err
	}
	t.lock.Lock()
	defer t.lock.Unlock()
	t.Target = target
	t.ceClient = ceClient
	return nil
}

func (t *Trigger) getFilter() filter.Filter {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.filter
}

func (t *Trigger) ChangeFilter(filters []*primitive.SubscriptionFilter) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.filter = filter.GetFilter(filters)
}

func (t *Trigger) getInputTransformer() *transformation.InputTransformer {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.inputTransformer
}

func (t *Trigger) ChangeInputTransformer(inputTransformer *primitive.InputTransformer) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if inputTransformer == nil || inputTransformer.Define == nil && inputTransformer.Template == "" {
		t.inputTransformer = nil
	} else {
		t.inputTransformer = transformation.NewInputTransformer(inputTransformer)
	}
}

func (t *Trigger) ChangeConfig(config primitive.SubscriptionConfig) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if config.RateLimit != 0 && config.RateLimit != t.config.RateLimit {
		t.applyOptions(WithRateLimit(config.RateLimit))
	}
}

func (t *Trigger) EventArrived(ctx context.Context, event info.EventRecord) error {
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
		timeout, cancel := context.WithTimeout(ctx, t.config.SendTimeOut)
		defer cancel()
		t.rateLimiter.Take()
		return t.getCeClient().Send(timeout, *e)
	}
	var err error
	inputTransformer := t.getInputTransformer()
	if inputTransformer != nil {
		err = inputTransformer.Execute(e)
		if err != nil {
			return err
		}
	}
	for retryTimes < t.config.MaxRetryTimes {
		retryTimes++
		if err = doFunc(); !ce.IsACK(err) {
			log.Debug(ctx, "process event error", map[string]interface{}{
				"error": err, "retryTimes": retryTimes,
			})
			time.Sleep(t.config.RetryPeriod)
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
		// TODO  是否立即停止，还是等待eventCh处理完.
		case <-ctx.Done():
			return
		case event, ok := <-t.eventCh:
			if !ok {
				return
			}
			if res := filter.Run(t.getFilter(), *event.Event); res == filter.FailFilter {
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
				log.Error(ctx, "send event to sink failed", map[string]interface{}{
					log.KeyError: err,
					"event":      event,
				})
			}
			t.offsetManager.EventCommit(event.OffsetInfo)
		}
	}
}

func (t *Trigger) runSleepWatch(ctx context.Context) {
	tk := time.NewTicker(sleepCheckPeriod)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.stateMutex.Lock()
			if t.state == TriggerRunning {
				if time.Since(t.lastActive) > t.SleepDuration {
					t.state = TriggerSleep
				} else {
					t.state = TriggerRunning
				}
			}
			t.stateMutex.Unlock()
		}
	}
}

func (t *Trigger) Start() error {
	ceClient, err := NewCeClient(t.Target)
	if err != nil {
		return err
	}
	t.ceClient = ceClient
	ctx, cancel := context.WithCancel(context.Background())
	t.stop = cancel
	for i := 0; i < t.config.FilterProcessSize; i++ {
		t.wg.StartWithContext(ctx, t.runEventProcess)
	}
	for i := 0; i < t.config.SendProcessSize; i++ {
		t.wg.StartWithContext(ctx, t.runEventSend)
	}
	t.state = TriggerRunning
	t.lastActive = time.Now()

	t.wg.StartWithContext(ctx, t.runSleepWatch)

	return nil
}

func (t *Trigger) Stop() {
	ctx := context.Background()
	log.Info(ctx, "trigger stop...", map[string]interface{}{
		log.KeySubscriptionID: t.SubscriptionID,
	})
	if t.GetState() == TriggerStopped {
		return
	}
	t.stop()
	t.wg.Wait()
	close(t.eventCh)
	close(t.sendCh)
	t.SetState(TriggerStopped)
	log.Info(ctx, "trigger stopped", map[string]interface{}{
		log.KeySubscriptionID: t.SubscriptionID,
	})
}

func (t *Trigger) GetState() State {
	t.stateMutex.RLock()
	defer t.stateMutex.RUnlock()
	return t.state
}

func (t *Trigger) SetState(state State) {
	t.stateMutex.Lock()
	defer t.stateMutex.Unlock()
	t.state = state
}
