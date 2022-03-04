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
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/consumer"
	"github.com/linkall-labs/vanus/internal/trigger/storage"
	"github.com/pkg/errors"
	"sync"
)

var (
	defaultEbVRN          = "vanus://192.168.1.111:2048/eventbus/wwf-0304-1?namespace=vanus"
	TriggerWorkerNotStart = errors.New("worker not start")
	SubExist              = errors.New("sub exist")
)

type Worker struct {
	subscriptions map[string]*subscriptionInfo
	lock          sync.RWMutex
	started       bool
	startedLock   sync.Mutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	config        Config
	storage       storage.OffsetStorage
}

type subscriptionInfo struct {
	sub           *primitive.Subscription
	trigger       *Trigger
	consumer      *consumer.Consumer
	offsetManager *consumer.EventLogOffset
}

func NewWorker(config Config) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		subscriptions: map[string]*subscriptionInfo{},
		ctx:           ctx,
		cancel:        cancel,
		config:        config,
	}
}

func (w *Worker) Start() error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if w.started {
		return nil
	}
	s, err := storage.NewOffsetStorage(w.config.Storage)
	if err != nil {
		return err
	}
	w.storage = s
	w.started = true
	return nil
}

func (w *Worker) Stop() error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return nil
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	var wg sync.WaitGroup
	for id := range w.subscriptions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.stopSub(id)
		}()
	}
	wg.Wait()
	w.cancel()
	w.storage.Close()
	for id := range w.subscriptions {
		w.cleanSub(id)
	}
	w.started = false
	return nil
}

func (w *Worker) stopSub(id string) {
	if info, exist := w.subscriptions[id]; exist {
		info.consumer.Close()
		info.trigger.Stop()
		info.offsetManager.Close()
	}
}

func (w *Worker) cleanSub(id string) {
	delete(w.subscriptions, id)
}

func (w *Worker) ListSubscription() []*primitive.Subscription {
	w.lock.RLock()
	defer w.lock.RUnlock()
	var list []*primitive.Subscription
	for _, sub := range w.subscriptions {
		list = append(list, sub.sub)
	}
	return list
}
func (w *Worker) AddSubscription(sub *primitive.Subscription) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	if _, exist := w.subscriptions[sub.ID]; exist {
		return SubExist
	}
	offsetManager := consumer.NewEventLogOffset(sub.ID, w.storage)
	offsetManager.Start(w.ctx)
	tr := NewTrigger(sub, offsetManager)
	ebVrn := sub.EventBus
	if ebVrn == "" {
		ebVrn = defaultEbVRN
	}
	c := consumer.NewConsumer(ebVrn, sub.ID, offsetManager, tr.EventArrived)
	err := c.Start(w.ctx)
	if err != nil {
		offsetManager.Close()
		return err
	}
	tr.Start(w.ctx)
	w.subscriptions[sub.ID] = &subscriptionInfo{
		sub:           sub,
		trigger:       tr,
		consumer:      c,
		offsetManager: offsetManager,
	}
	return nil
}

func (w *Worker) PauseSubscription(id string) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.lock.RLock()
	defer w.lock.RUnlock()
	w.stopSub(id)
	return nil
}

func (w *Worker) RemoveSubscription(id string) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	if _, exist := w.subscriptions[id]; !exist {
		return nil
	}
	w.stopSub(id)
	w.cleanSub(id)
	return nil
}
