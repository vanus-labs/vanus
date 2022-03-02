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
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/inmemory"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/consumer"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var (
	ebVRN                 = "vanus+local:eventbus:example"
	TriggerWorkerNotStart = errors.New("worker not start")
	SubExist              = errors.New("sub exist")
)

type Worker struct {
	subscriptions map[string]*primitive.Subscription
	triggers      map[string]*Trigger
	consumers     map[string]*consumer.Consumer
	stopCh        chan struct{}
	lock          sync.RWMutex
	started       bool
	startedLock   sync.Mutex
	wg            sync.WaitGroup
}

func NewWorker() *Worker {
	return &Worker{
		subscriptions: map[string]*primitive.Subscription{},
		triggers:      map[string]*Trigger{},
		consumers:     map[string]*consumer.Consumer{},
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
		defer tk.Stop()
		for {
			select {
			case <-w.stopCh:
				return
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
	w.lock.Lock()
	defer w.lock.Unlock()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for id := range w.subscriptions {
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.stopSub(id)
			}()
		}
	}()
	wg.Wait()
	for id := range w.subscriptions {
		w.cleanSub(id)
	}
	w.started = false
	return nil
}

func (w *Worker) stopSub(id string) {
	if c, exist := w.consumers[id]; exist {
		c.Close()
	}
	if tr, exist := w.triggers[id]; exist {
		tr.Stop(context.Background())
	}
}

func (w *Worker) cleanSub(id string) {
	delete(w.consumers, id)
	delete(w.triggers, id)
	delete(w.subscriptions, id)
}

func (w *Worker) ListSubscription() []*primitive.Subscription {
	w.lock.RLock()
	w.lock.RUnlock()
	var list []*primitive.Subscription
	for _, sub := range w.subscriptions {
		list = append(list, sub)
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

func (w *Worker) run() {
	w.lock.RLock()
	defer w.lock.RUnlock()
	if len(w.subscriptions) == 0 {
		return
	}
	for _, sub := range w.subscriptions {
		tr := w.triggers[sub.ID]
		if tr.state != TriggerRunning {
			continue
		}
		c, ok := w.consumers[sub.ID]
		if !ok {
			c = consumer.NewConsumer(ebVRN, sub.ID, tr.EventArrived)
			c.Start()
			w.consumers[sub.ID] = c
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
		for ; i < 10000; i++ {
			if i%3 == 0 {
				t = "test"
			}
			if i%2 == 0 {
				time.Sleep(10 * time.Second)
				t = "none"
			}
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType(t)
			event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world", "type": t})

			_, err = w.Append(&event)
			if err != nil {
				log.Error("append event error", map[string]interface{}{"error": err})
			}
		}
	}()
	go http.ListenAndServe(":18080", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			fmt.Println(err)
		}
		log.Info("sink receive event", map[string]interface{}{
			"event": string(body),
		})
	}))
}
