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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"math"
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
	TriggerProcessorCreated  TriggerProcessorState = "created"
	TriggerProcessorPending  TriggerProcessorState = "pending"
	TriggerProcessorRunning  TriggerProcessorState = "running"
	TriggerProcessorStopping TriggerProcessorState = "stopping"
	TriggerProcessorStop     TriggerProcessorState = "stop"
)

//TriggerProcessor send Subscription to trigger worker
type TriggerProcessor struct {
	twAddr        string
	twClient      trigger.TriggerWorkerClient
	subscriptions map[string]*primitive.Subscription
	subLock       sync.Mutex
	stateMutex    sync.RWMutex
	state         TriggerProcessorState
}

func NewTriggerProcessor(addr string) (*TriggerProcessor, error) {
	twClient, err := NewTriggerWorkerClient(addr)
	if err != nil {
		return nil, err
	}
	return &TriggerProcessor{
		twClient:      twClient,
		twAddr:        addr,
		subscriptions: map[string]*primitive.Subscription{},
		state:         TriggerProcessorCreated,
	}, nil
}

func (p *TriggerProcessor) setState(state TriggerProcessorState) {
	p.stateMutex.Lock()
	p.stateMutex.Unlock()
	p.state = state
}

func (p *TriggerProcessor) getState() TriggerProcessorState {
	p.stateMutex.RLock()
	p.stateMutex.RUnlock()
	return p.state
}
func (p *TriggerProcessor) Stop() {
	p.setState(TriggerProcessorStopping)
	p.StopTriggerWorker()
	p.setState(TriggerProcessorStop)
}

func (p *TriggerProcessor) Start() error {
	err := p.StartTriggerWorker()
	if err != nil {
		return err
	}
	return nil
}

func (p *TriggerProcessor) StartTriggerWorker() (err error) {
	defer func() {
		if err != nil {
			p.setState(TriggerProcessorCreated)
		}
	}()
	p.setState(TriggerProcessorPending)
	request := &trigger.StartTriggerWorkerRequest{}
	_, err = p.twClient.Start(context.Background(), request)
	if err != nil {
		log.Error("rpc start trigger worker error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	p.setState(TriggerProcessorRunning)
	return nil
}

func (p *TriggerProcessor) StopTriggerWorker() error {
	request := &trigger.StopTriggerWorkerRequest{}
	_, err := p.twClient.Stop(context.Background(), request)
	if err != nil {
		log.Error("rpc stop trigger worker error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	p.setState(TriggerProcessorStop)
	return nil
}

func (p *TriggerProcessor) AddSubscription(sub *primitive.Subscription) error {
	if sub == nil {
		return nil
	}
	p.subLock.Lock()
	defer p.subLock.Unlock()
	ctx := context.Background()
	if _, ok := p.subscriptions[sub.ID]; !ok {
		request, err := func(sub *primitive.Subscription) (*trigger.AddSubscriptionRequest, error) {
			b, err := json.Marshal(sub)
			if err != nil {
				return nil, fmt.Errorf("marshal error %v", err)
			}
			pSub := &trigger.Subscription{}
			err = json.Unmarshal(b, pSub)
			if err != nil {
				return nil, fmt.Errorf("unmarshal error, json %s", string(b))
			}
			request := &trigger.AddSubscriptionRequest{
				Subscription: pSub,
			}
			return request, nil
		}(sub)
		if err != nil {
			return fmt.Errorf("add subscription error %v", err.Error())
		}
		_, err = p.twClient.AddSubscription(ctx, request)
		if err != nil {
			return fmt.Errorf("add subscription error %v", err)
		}
	} else {
		request := &trigger.ResumeSubscriptionRequest{}
		_, err := p.twClient.ResumeSubscription(ctx, request)
		if err != nil {
			return fmt.Errorf("resume subscription error %v", err)
		}
	}
	p.subscriptions[sub.ID] = sub
	return nil
}

func (p *TriggerProcessor) RemoveSubscriptions(id string) error {
	p.subLock.Lock()
	defer p.subLock.Unlock()
	request := &trigger.RemoveSubscriptionRequest{Id: id}
	_, err := p.twClient.RemoveSubscription(context.Background(), request)
	if err != nil {
		log.Error("remove subscription error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	delete(p.subscriptions, id)
	return nil
}

func (p *TriggerProcessor) PauseSubscriptions(id string) error {
	p.subLock.Lock()
	defer p.subLock.Unlock()
	request := &trigger.PauseSubscriptionRequest{Id: id}
	_, err := p.twClient.PauseSubscription(context.Background(), request)
	if err != nil {
		log.Error("rpc pause subscription error", map[string]interface{}{"addr": p.twAddr, "request": request})
		return err
	}
	return nil
}

//TriggerController allocate subscription to trigger processor
type TriggerController struct {
	triggerProcessors    map[string]*TriggerProcessor
	pendingSubscriptions map[string]*primitive.Subscription
	stopCh               chan struct{}
	mutex                sync.Mutex
	pMutex               sync.RWMutex
}

func NewTriggerController() *TriggerController {
	return &TriggerController{
		pendingSubscriptions: map[string]*primitive.Subscription{},
		triggerProcessors:    map[string]*TriggerProcessor{},
		stopCh:               make(chan struct{}),
	}
}

func (tc *TriggerController) Start() {
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		for {
			select {
			case <-tc.stopCh:
				log.Info("trigger processor controller exist", nil)
				return
			case <-tk.C:
				tc.processSubscriptions()
			}
		}
	}()
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		for {
			select {
			case <-tc.stopCh:
				log.Info("trigger processor controller exist", nil)
				return
			case <-tk.C:
				tc.checkTriggerProcessorRunning()
			}
		}
	}()
}

func (tc *TriggerController) Stop() {
	close(tc.stopCh)
}

func (tc *TriggerController) AddSubscription(subscription *primitive.Subscription) {
	if subscription == nil {
		return
	}
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	tc.pendingSubscriptions[subscription.ID] = subscription
}

func (tc *TriggerController) processSubscriptions() {
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	if len(tc.pendingSubscriptions) == 0 {
		return
	}
	for key := range tc.pendingSubscriptions {
		p := tc.findTriggerProcessor()
		if p == nil {
			log.Debug("process subscriptions no found trigger processor", nil)
			return
		}
		err := p.AddSubscription(tc.pendingSubscriptions[key])
		if err != nil {
			log.Error("add subscription error", map[string]interface{}{"addr": p.twAddr, "error": err})
			continue
		}
		delete(tc.pendingSubscriptions, key)
	}
}

func (tc *TriggerController) addTriggerProcessor(p *TriggerProcessor) {
	tc.pMutex.Lock()
	defer tc.pMutex.Unlock()
	tc.triggerProcessors[p.twAddr] = p
}

func (tc *TriggerController) removeTriggerProcessor(addr string) {
	tc.pMutex.Lock()
	defer tc.pMutex.Unlock()
	if p, ok := tc.triggerProcessors[addr]; ok {
		p.Stop()
		delete(tc.triggerProcessors, addr)
	}
}

func (tc *TriggerController) findTriggerProcessor() *TriggerProcessor {
	tc.pMutex.RLock()
	defer tc.pMutex.RUnlock()
	if len(tc.triggerProcessors) == 0 {
		return nil
	}
	//TODO 算法
	var key string
	c := math.MaxInt16
	for k, p := range tc.triggerProcessors {
		if p.getState() != TriggerProcessorRunning {
			continue
		}
		curr := len(p.subscriptions)
		if curr < c {
			key = k
		}
	}
	if key == "" {
		return nil
	}
	return tc.triggerProcessors[key]
}

func (tc *TriggerController) checkTriggerProcessorRunning() {
	tc.pMutex.RLock()
	defer tc.pMutex.RUnlock()
	for _, p := range tc.triggerProcessors {
		switch p.getState() {
		case TriggerProcessorCreated:
			err := p.Start()
			if err != nil {
				log.Error("start trigger processor error", map[string]interface{}{"err": err.Error()})
				continue
			}
		}
	}
}

func (tc *TriggerController) AddTriggerProcessor(addr string) error {
	p, err := NewTriggerProcessor(addr)
	if err != nil {
		return fmt.Errorf("new trigger processor error %v", err)
	}
	tc.addTriggerProcessor(p)
	return nil
}
