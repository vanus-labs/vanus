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
	"errors"
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/inmemory"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
	"time"
)

var (
	ebVRN                 = "vanus+local:eventbus:example"
	TriggerWorkerNotStart = errors.New("worker not start")
)

type Worker struct {
	subscriptions map[string]*primitive.Subscription
	triggers      map[string]*Trigger
	consumers     map[string]*Consumer
	eventBus      map[string]string
	stopCh        chan struct{}
	trLock        sync.RWMutex
	started       bool
	startedLock   sync.Mutex
	wg            sync.WaitGroup
}

func NewWorker() *Worker {
	return &Worker{
		subscriptions: map[string]*primitive.Subscription{},
		triggers:      map[string]*Trigger{},
		consumers:     map[string]*Consumer{},
		stopCh:        make(chan struct{}),
	}
}

func (w *Worker) Start() error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if w.started {
		return nil
	}
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		for {
			select {
			case <-w.stopCh:
				log.Info("worker stop", nil)
			case <-tk.C:
				w.run()
			}
		}
	}()
	testSend()
	w.started = true
	return nil
}

func (w *Worker) Stop() error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return nil
	}
	close(w.stopCh)
	w.trLock.Lock()
	defer w.trLock.Unlock()
	for id, tr := range w.triggers {
		tr.Stop(context.Background())
		delete(w.triggers, id)
		delete(w.subscriptions, id)
	}
	w.started = false
	return nil
}

func (w *Worker) AddSubscription(sub *primitive.Subscription) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.trLock.Lock()
	defer w.trLock.Unlock()
	w.subscriptions[sub.ID] = sub
	tr := NewTrigger(sub)
	w.triggers[sub.ID] = tr
	return tr.Start(context.Background())
}

func (w *Worker) PauseSubscription(id string) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.trLock.RLock()
	defer w.trLock.RUnlock()
	if tr, ok := w.triggers[id]; ok {
		tr.Stop(context.Background())
	}
	return nil
}

func (w *Worker) RemoveSubscription(id string) error {
	w.startedLock.Lock()
	defer w.startedLock.Unlock()
	if !w.started {
		return TriggerWorkerNotStart
	}
	w.trLock.Lock()
	defer w.trLock.Unlock()
	if _, ok := w.subscriptions[id]; ok {
		delete(w.subscriptions, id)
	}
	if tr, ok := w.triggers[id]; ok {
		tr.Stop(context.Background())
		delete(w.triggers, id)
	}
	return nil
}

func (w *Worker) run() {
	w.trLock.RLock()
	defer w.trLock.RUnlock()
	if len(w.triggers) == 0 {
		return
	}
	for _, tr := range w.triggers {
		if tr.state != TriggerRunning {
			continue
		}
		c, ok := w.consumers[tr.ID]
		if !ok {
			if curr, err := NewConsumer(ebVRN); err != nil {
				log.Error("new consumer error", map[string]interface{}{"ebVRN": ebVRN, "error": err})
				continue
			} else {
				c = curr
				w.consumers[tr.ID] = c
			}
		}
		events := c.messages(5)
		for i := range events {
			tr.EventArrived(context.Background(), events[i])
		}
	}
}

func (w *Worker) fetch() {

}

func testSend() {

	elVRN := "vanus+local:eventlog+inmemory:1?keepalive=true"
	elVRN2 := "vanus+local:eventlog+inmemory:2?keepalive=true"
	br := &record.EventBus{
		VRN: ebVRN,
		Logs: []*record.EventLog{
			{
				VRN:  elVRN,
				Mode: record.PremWrite | record.PremRead,
			},
			{
				VRN:  elVRN2,
				Mode: record.PremWrite | record.PremRead,
			},
		},
	}

	ns := discovery.Find("vanus+local").(*inmemory.NameService)
	// register metadata of eventbus
	ns.Register(ebVRN, br)
	w, err := eb.OpenBusWriter(ebVRN)
	if err != nil {
		log.Fatal("open bus writer error", map[string]interface{}{"error": err})
	}

	go func() {
		i := 1
		t := "none"
		for ; i < 1000; i++ {
			if i%10 == 0 {
				t = "test"
			}
			if i%6 == 0 {
				time.Sleep(10 * time.Second)
				t = "none"
			}
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType(t)
			event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world", "type": "none"})

			_, err = w.Append(&event)
			if err != nil {
				log.Error("append event error", map[string]interface{}{"error": err})
			}
		}
	}()
}
