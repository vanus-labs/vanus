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

package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/worker"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"time"
)

func NewTriggerWorkerClient(target string) (trigger.TriggerWorkerClient, error) {
	cc, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	twClient := trigger.NewTriggerWorkerClient(cc)
	return twClient, nil
}

type TriggerProcessorState string

var (
	TriggerProcessorCreated TriggerProcessorState = "created"
	TriggerProcessorRunning TriggerProcessorState = "running"
	TriggerProcessorStop    TriggerProcessorState = "stop"
)

//TriggerProcessor send Subscription to trigger worker
type TriggerProcessor struct {
	twClient             trigger.TriggerWorkerClient
	runningSubscriptions map[string]*primitive.Subscription
	pendingSubscriptions map[string]*primitive.Subscription
	triggerStates        map[string]worker.TriggerState
	twAddr               string
	pendingMutex         sync.Mutex
	runningMutex         sync.Mutex
	stopCh               chan struct{}
}

func NewTriggerProcessor(addr string) (*TriggerProcessor, error) {
	twClient, err := NewTriggerWorkerClient(addr)
	if err != nil {
		return nil, err
	}
	return &TriggerProcessor{
		twClient:             twClient,
		twAddr:               addr,
		runningSubscriptions: map[string]*primitive.Subscription{},
		pendingSubscriptions: map[string]*primitive.Subscription{},
		triggerStates:        map[string]worker.TriggerState{},
		stopCh:               make(chan struct{}),
	}, nil
}

func (p *TriggerProcessor) Stop() {
	close(p.stopCh)
}

func (p *TriggerProcessor) Start() error {
	err := p.StartTriggerWorker()
	if err != nil {
		return err
	}
	go p.scheduleSubscription()
	return nil
}

func (p *TriggerProcessor) StartTriggerWorker() error {
	request := &trigger.StartTriggerWorkerRequest{}
	_, err := p.twClient.Start(context.Background(), request)
	if err != nil {
		log.Error("rpc start trigger worker error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	return nil
}

func (p *TriggerProcessor) StopTriggerWorker() error {
	request := &trigger.StopTriggerWorkerRequest{}
	_, err := p.twClient.Stop(context.Background(), request)
	if err != nil {
		log.Error("rpc stop trigger worker error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	return nil
}

func (p *TriggerProcessor) AddSubscription(subscription *primitive.Subscription) {
	if subscription == nil {
		return
	}
	p.pendingMutex.Lock()
	p.pendingMutex.Unlock()
	p.pendingSubscriptions[subscription.ID] = subscription
}

func (p *TriggerProcessor) scheduleSubscription() {
	tk := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-p.stopCh:
			log.Info("trigger processor receive stop", nil)
			return
		case <-tk.C:
			p.processSubscriptions()
		}
	}
}

func (p *TriggerProcessor) processSubscriptions() {
	p.pendingMutex.Lock()
	defer p.pendingMutex.Unlock()
	if len(p.pendingSubscriptions) == 0 {
		return
	}
	for key := range p.pendingSubscriptions {
		subscription := p.pendingSubscriptions[key]
		err := p.runSubscriptions(subscription)
		if err != nil {
			log.Error("run subscription error", map[string]interface{}{
				"addr":         p.twAddr,
				"subscription": subscription,
				"error":        err,
			})
			continue
		}
		delete(p.pendingSubscriptions, key)
	}
}
func (p *TriggerProcessor) runSubscriptions(subscription *primitive.Subscription) error {
	p.runningMutex.Lock()
	defer p.runningMutex.Unlock()
	ctx := context.Background()
	if _, ok := p.runningSubscriptions[subscription.ID]; !ok {
		b, err := json.Marshal(subscription)
		if err != nil {
			return fmt.Errorf("marshal error %v", err)
		}
		pSub := &trigger.Subscription{}
		err = json.Unmarshal(b, pSub)
		if err != nil {
			return fmt.Errorf("unmarshal error, json %s", string(b))
		}
		request := &trigger.AddSubscriptionRequest{
			Subscription: pSub,
		}
		_, err = p.twClient.AddSubscription(ctx, request)
		if err != nil {
			return fmt.Errorf("prc add subscription error %v", err)
		}
	} else {
		request := &trigger.ResumeSubscriptionRequest{}
		_, err := p.twClient.ResumeSubscription(ctx, request)
		if err != nil {
			return fmt.Errorf("prc resume subscription error %v", err)
		}
	}
	p.runningSubscriptions[subscription.ID] = subscription
	return nil
}

func (p *TriggerProcessor) removeSubscriptions(id string) error {
	p.runningMutex.Lock()
	defer p.runningMutex.Unlock()
	request := &trigger.RemoveSubscriptionRequest{Id: id}
	_, err := p.twClient.RemoveSubscription(context.Background(), request)
	if err != nil {
		log.Error("rpc remove subscription error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	delete(p.runningSubscriptions, id)
	return nil
}

func (p *TriggerProcessor) pauseSubscriptions(id string) error {
	p.runningMutex.Lock()
	defer p.runningMutex.Unlock()
	request := &trigger.PauseSubscriptionRequest{Id: id}
	_, err := p.twClient.PauseSubscription(context.Background(), request)
	if err != nil {
		log.Error("rpc pause subscription error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	return nil
}

//TriggerProcessorManager allocate subscription to trigger processor
//autoscaling trigger processor
type TriggerProcessorManager struct {
	runningTriggerProcessors map[string]*TriggerProcessor
	pendingTriggerProcessors map[string]*TriggerProcessor
	PendingSubscriptions     map[string]*primitive.Subscription
	stopCh                   chan struct{}
	mutex                    sync.Mutex
}

func NewTriggerProcessorManager() *TriggerProcessorManager {
	return &TriggerProcessorManager{
		PendingSubscriptions:     map[string]*primitive.Subscription{},
		pendingTriggerProcessors: map[string]*TriggerProcessor{},
		runningTriggerProcessors: map[string]*TriggerProcessor{},
		stopCh:                   make(chan struct{}),
	}
}

func (m *TriggerProcessorManager) Start() {
	go m.allocateSubscriptions()
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		for {
			select {
			case <-m.stopCh:
				log.Info("trigger processor manager exist", nil)
				return
			case <-tk.C:
				m.checkTriggerProcessorRunning()
			}
		}
	}()
}

func (m *TriggerProcessorManager) Stop() {
	close(m.stopCh)
}

func (m *TriggerProcessorManager) AddSubscription(subscription *primitive.Subscription) {
	if subscription == nil {
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.PendingSubscriptions[subscription.ID] = subscription
}

func (m *TriggerProcessorManager) allocateSubscriptions() {
	tk := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case <-m.stopCh:
			log.Info("trigger processor manager exist", nil)
			return
		case <-tk.C:
			m.processSubscriptions()
		}
	}
}

func (m *TriggerProcessorManager) processSubscriptions() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if len(m.PendingSubscriptions) == 0 {
		return
	}
	for key := range m.PendingSubscriptions {
		p := m.findRunningTriggerProcessor()
		if p == nil {
			log.Info("process subscriptions no found trigger processor", nil)
			return
		}
		p.AddSubscription(m.PendingSubscriptions[key])
		delete(m.PendingSubscriptions, key)
	}
}

func (m *TriggerProcessorManager) findRunningTriggerProcessor() *TriggerProcessor {
	//TODO 算法
	for _, p := range m.runningTriggerProcessors {
		return p
	}
	return nil
}

func (m *TriggerProcessorManager) checkTriggerProcessorRunning() {
	for k, p := range m.pendingTriggerProcessors {
		err := p.Start()
		if err != nil {
			log.Error("start processor error", map[string]interface{}{"err": err.Error()})
			continue
		}
		delete(m.pendingTriggerProcessors, k)
		m.runningTriggerProcessors[k] = p
	}
}

func (m *TriggerProcessorManager) AddTriggerProcessor(addr string) error {
	p, err := NewTriggerProcessor(addr)
	if err != nil {
		return fmt.Errorf("new trigger processor error %v", err)
	}
	m.pendingTriggerProcessors[addr] = p
	return nil
}
