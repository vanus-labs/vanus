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
	"strings"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	"github.com/linkall-labs/vanus/internal/trigger/trigger"
	"github.com/linkall-labs/vanus/observability/log"
)

type Worker struct {
	subscriptions map[vanus.ID]*subscriptionWorker
	offsetManager *offset.Manager
	lock          sync.RWMutex
	wg            sync.WaitGroup
	ctx           context.Context
	stop          context.CancelFunc
	config        Config
}

type subscriptionWorker struct {
	sub      *primitive.Subscription
	trigger  *trigger.Trigger
	events   chan info.EventOffset
	reader   reader.Reader
	stopTime time.Time
}

func (wk *Worker) NewSubWorker(sub *primitive.Subscription, subOffset *offset.SubscriptionOffset) *subscriptionWorker {
	w := &subscriptionWorker{
		events: make(chan info.EventOffset, 2048),
		sub:    sub,
	}
	offset := make(map[vanus.ID]uint64)
	for _, o := range sub.Offsets {
		offset[o.EventLogID] = o.Offset
	}
	w.reader = reader.NewReader(wk.getReaderConfig(sub), offset, w.events)
	triggerConf := &trigger.Config{}
	w.trigger = trigger.NewTrigger(triggerConf, sub, subOffset)
	return w
}

func (w *subscriptionWorker) Run(ctx context.Context) error {
	err := w.reader.Start()
	if err != nil {
		return err
	}
	err = w.trigger.Start()
	if err != nil {
		return err
	}
	go func() {
		for event := range w.events {
			_ = w.trigger.EventArrived(ctx, info.EventRecord{EventOffset: event})
		}
	}()
	return nil
}

func NewWorker(config Config) *Worker {
	if config.CleanSubscriptionTimeout == 0 {
		config.CleanSubscriptionTimeout = 5 * time.Second
	}
	w := &Worker{
		subscriptions: map[vanus.ID]*subscriptionWorker{},
		offsetManager: offset.NewOffsetManager(),
		config:        config,
	}
	w.ctx, w.stop = context.WithCancel(context.Background())
	return w
}

func (w *Worker) Start() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return nil
}

func (w *Worker) Stop() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	var wg sync.WaitGroup
	for id := range w.subscriptions {
		wg.Add(1)
		go func(id vanus.ID) {
			defer wg.Done()
			w.stopSubscription(id)
			w.cleanSubscription(id)
		}(id)
	}
	wg.Wait()
	w.stop()
	return nil
}

func (w *Worker) stopSubscription(id vanus.ID) {
	if info, exist := w.subscriptions[id]; exist {
		log.Info(w.ctx, "worker begin stop subscription", map[string]interface{}{
			"subId": id,
		})
		info.reader.Close()
		close(info.events)
		info.trigger.Stop()
		log.Info(w.ctx, "worker success stop subscription", map[string]interface{}{
			"subId": id,
		})
		info.stopTime = time.Now()
	}
}

func (w *Worker) cleanSubscription(id vanus.ID) {
	//wait offset commit or timeout
	ctx, cancel := context.WithTimeout(context.Background(), w.config.CleanSubscriptionTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			info, exist := w.subscriptions[id]
			if !exist || info.stopTime.Before(w.offsetManager.GetLastCommitTime()) {
				break loop
			}
		}
	}
	delete(w.subscriptions, id)
	w.offsetManager.RemoveSubscription(id)
}

func (w *Worker) ListSubscriptionInfo() ([]pInfo.SubscriptionInfo, func()) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	var list []pInfo.SubscriptionInfo
	for id := range w.subscriptions {
		subOffset := w.offsetManager.GetSubscription(id)
		if subOffset == nil {
			continue
		}
		list = append(list, pInfo.SubscriptionInfo{
			SubscriptionID: id,
			Offsets:        subOffset.GetCommit(),
		})
	}
	return list, func() {
		w.offsetManager.SetLastCommitTime()
	}
}

func (w *Worker) AddSubscription(sub *primitive.Subscription) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if _, exist := w.subscriptions[sub.ID]; exist {
		return errors.ErrResourceAlreadyExist
	}
	subOffset := w.offsetManager.RegisterSubscription(sub.ID)
	subWorker := w.NewSubWorker(sub, subOffset)
	err := subWorker.Run(w.ctx)
	if err != nil {
		w.offsetManager.RemoveSubscription(sub.ID)
		return err
	}
	w.subscriptions[sub.ID] = subWorker
	return nil
}

func (w *Worker) PauseSubscription(id vanus.ID) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.stopSubscription(id)
	return nil
}

func (w *Worker) RemoveSubscription(id vanus.ID) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	if _, exist := w.subscriptions[id]; !exist {
		return nil
	}
	w.stopSubscription(id)
	w.cleanSubscription(id)
	return nil
}

func (w *Worker) getReaderConfig(sub *primitive.Subscription) reader.Config {
	ebVrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s",
		w.config.Controllers[0], sub.EventBus,
		strings.Join(w.config.Controllers, ","))
	return reader.Config{
		EventBusName: sub.EventBus,
		EventBusVRN:  ebVrn,
		SubId:        sub.ID,
	}
}
