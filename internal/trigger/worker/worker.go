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
	"os"
	"sync"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/inmemory"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/consumer"
	"github.com/linkall-labs/vanus/internal/trigger/storage"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
)

var (
	defaultEbVRN          = "vanus+local:eventbus:example"
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
	w := &Worker{
		subscriptions: map[string]*subscriptionInfo{},
		config:        config,
	}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	return w
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
	testSend()
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
		go func(id string) {
			defer wg.Done()
			w.stopSub(id)
		}(id)
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
		log.Info(w.ctx, "worker begin stop subscription", map[string]interface{}{
			"subId": id,
		})
		info.consumer.Close()
		info.trigger.Stop()
		info.offsetManager.Close()
		log.Info(w.ctx, "worker success stop subscription", map[string]interface{}{
			"subId": id,
		})
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
	err = tr.Start(w.ctx)
	if err != nil {
		c.Close()
		offsetManager.Close()
	}
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

func testSend() {
	ebVRN := "vanus+local:eventbus:example"
	elVRN := "vanus+inmemory:eventlog:1"
	br := &record.EventBus{
		VRN: ebVRN,
		Logs: []*record.EventLog{
			{
				VRN:  elVRN,
				Mode: record.PremWrite | record.PremRead,
			},
		},
	}

	inmemory.UseInMemoryLog("vanus+inmemory")
	ns := inmemory.UseNameService("vanus+local")
	// register metadata of eventbus
	vrn, err := discovery.ParseVRN(ebVRN)
	if err != nil {
		panic(err.Error())
	}
	ns.Register(vrn, br)
	bw, err := eb.OpenBusWriter(ebVRN)
	if err != nil {
		log.Error(context.Background(), "open bus writer error", map[string]interface{}{"error": err})
		os.Exit(1)
	}

	go func() {
		i := 1
		tp := "none"
		for ; i < 10000; i++ {
			if i%3 == 0 {
				tp = "test"
			}
			if i%2 == 0 {
				time.Sleep(10 * time.Second)
				tp = "none"
			}
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType(tp)
			event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world", "type": tp})

			_, err = bw.Append(context.Background(), &event)
			if err != nil {
				log.Error(context.Background(), "append event error", map[string]interface{}{"error": err})
			}
		}
	}()
}
