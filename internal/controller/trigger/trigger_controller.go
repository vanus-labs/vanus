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
	"encoding/json"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math"
	"sync"
	"time"
)

type subscriptionTrigger struct {
	Sub     *primitive.Subscription
	tWorker *triggerWorker
}

//triggerController allocate subscription to trigger processor
type triggerController struct {
	heartbeats           map[string]time.Time
	triggerWorkers       map[string]*triggerWorker
	subscriptions        map[string]*subscriptionTrigger
	pendingSubscriptions map[string]*primitive.Subscription
	pendingSubMutex      sync.Mutex
	subMutex             sync.Mutex
	twMutex              sync.RWMutex
	ctx                  context.Context
	cancel               context.CancelFunc
}

func NewTriggerController() *triggerController {
	ctx, cancel := context.WithCancel(context.Background())
	return &triggerController{
		pendingSubscriptions: map[string]*primitive.Subscription{},
		triggerWorkers:       map[string]*triggerWorker{},
		subscriptions:        map[string]*subscriptionTrigger{},
		ctx:                  ctx,
		cancel:               cancel,
	}
}

func (ctrl *triggerController) CreateSubscription(ctx context.Context, request *ctrlpb.CreateSubscriptionRequest) (*meta.Subscription, error) {
	sub, err := func(request *ctrlpb.CreateSubscriptionRequest) (*primitive.Subscription, error) {
		b, err := json.Marshal(request)
		if err != nil {
			return nil, errors.Wrap(err, "marshal error")
		}
		sub := &primitive.Subscription{}
		err = json.Unmarshal(b, sub)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal error, json %s", string(b))
		}
		return sub, nil
	}(request)
	if err != nil {
		return nil, err
	}
	sub.ID = uuid.NewString()
	err = ctrl.addSubscription(sub)
	if err != nil {
		return nil, err
	}
	resp, err := convert.InnerSubToMetaSub(sub)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func (ctrl *triggerController) DeleteSubscription(ctx context.Context, request *ctrlpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	err := ctrl.deleteSubscription(request.Id)
	if err != nil {
		log.Error("delete subscription failed", map[string]interface{}{
			"subId":      request.Id,
			log.KeyError: err,
		})
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (ctrl *triggerController) GetSubscription(ctx context.Context, request *ctrlpb.GetSubscriptionRequest) (*meta.Subscription, error) {
	sub := ctrl.getSubscription(request.Id)
	resp, _ := convert.InnerSubToMetaSub(sub)
	return resp, nil
}
func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	for {
		req, err := heartbeat.Recv()
		if err != nil {
			ctrl.heartbeats[req.Address] = time.Now()
		} else {
			if err == io.EOF {
				log.Info("heartbeat receive close", nil)
				return nil
			}
			log.Info("heartbeat recv error", map[string]interface{}{log.KeyError: err})
			return err
		}
	}
}

func (ctrl *triggerController) RegisterTriggerWorker(ctx context.Context, request *ctrlpb.RegisterTriggerWorkerRequest) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	err := ctrl.addTriggerWorker(request.Address)
	if err != nil {
		log.Error("add trigger worker failed", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) UnregisterTriggerWorker(ctx context.Context, request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	ctrl.removeTriggerWorker(request.Address)
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) addSubscription(sub *primitive.Subscription) error {
	ctrl.pendingSubMutex.Lock()
	ctrl.pendingSubMutex.Unlock()
	ctrl.pendingSubscriptions[sub.ID] = sub
	return nil
}

func (ctrl *triggerController) deleteSubscription(id string) error {
	ctrl.pendingSubMutex.Lock()
	if _, ok := ctrl.pendingSubscriptions[id]; ok {
		delete(ctrl.pendingSubscriptions, id)
	}
	ctrl.pendingSubMutex.Unlock()
	ctrl.subMutex.Lock()
	defer ctrl.subMutex.Unlock()
	st, ok := ctrl.subscriptions[id]
	if !ok {
		return nil
	}
	err := st.tWorker.RemoveSubscriptions(id)
	if err != nil {
		return errors.Wrapf(err, "trigger worker remove sub %s error", id)
	}
	delete(ctrl.subscriptions, id)
	return nil
}

func (ctrl *triggerController) getSubscription(id string) *primitive.Subscription {
	ctrl.pendingSubMutex.Lock()
	if sub, ok := ctrl.pendingSubscriptions[id]; ok {
		ctrl.pendingSubMutex.Unlock()
		return sub
	}
	ctrl.pendingSubMutex.Unlock()
	ctrl.subMutex.Lock()
	defer ctrl.subMutex.Unlock()
	st, ok := ctrl.subscriptions[id]
	if !ok {
		return nil
	}
	return st.Sub
}

func (ctrl *triggerController) addTriggerWorker(addr string) error {
	ctrl.twMutex.Lock()
	ctrl.twMutex.Unlock()
	tWorker := NewTriggerWorker(addr)
	err := tWorker.Start()
	if err != nil {
		return errors.Wrapf(err, "trigger worker %s start error", addr)
	}
	ctrl.triggerWorkers[addr] = tWorker
	log.Info("add a trigger worker", map[string]interface{}{"twAddr": addr})
	return nil
}

//receive trigger worker stop signal
func (ctrl *triggerController) removeTriggerWorker(addr string) {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	tWorker, ok := ctrl.triggerWorkers[addr]
	if !ok {
		log.Info("remove trigger worker but not exist", map[string]interface{}{"" +
			"twAddr": addr,
		})
		return
	}
	ctrl.pendingSubMutex.Lock()
	defer ctrl.pendingSubMutex.Unlock()
	//rejoin pending sub
	for id, sub := range tWorker.subscriptions {
		ctrl.pendingSubscriptions[id] = sub
	}
	delete(ctrl.triggerWorkers, addr)
	tWorker.Close()
	log.Info("remove a trigger worker", map[string]interface{}{"twAddr": addr})
}

func (ctrl *triggerController) Start() {
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctrl.ctx.Done():
				return
			case <-tk.C:
				ctrl.processSubscriptions()
			}
		}
	}()
	go func() {
		tk := time.NewTicker(1 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctrl.ctx.Done():
				return
			case <-tk.C:
				//TODO heartbeat check
			}
		}
	}()
}

func (ctrl *triggerController) Close() error {
	ctrl.cancel()
	return nil
}

func (ctrl *triggerController) processSubscriptions() {
	ctrl.pendingSubMutex.Lock()
	defer ctrl.pendingSubMutex.Unlock()
	if len(ctrl.pendingSubscriptions) == 0 {
		return
	}
	for key := range ctrl.pendingSubscriptions {
		tWorker := ctrl.findTriggerWorker()
		if tWorker == nil {
			log.Debug("process subscriptions no found trigger processor", nil)
			return
		}
		err := tWorker.AddSubscription(ctrl.pendingSubscriptions[key])
		if err != nil {
			log.Error("add subscription error", map[string]interface{}{
				"addr":       tWorker.twAddr,
				log.KeyError: err,
			})
			continue
		}
		log.Info("allocate a sub to triggerWorker", map[string]interface{}{
			"subId":  key,
			"twAddr": tWorker.twAddr,
		})
		ctrl.subMutex.Lock()
		ctrl.subscriptions[key] = &subscriptionTrigger{
			Sub:     ctrl.pendingSubscriptions[key],
			tWorker: tWorker,
		}
		ctrl.subMutex.Unlock()
		delete(ctrl.pendingSubscriptions, key)
	}
}

func (ctrl *triggerController) findTriggerWorker() *triggerWorker {
	ctrl.twMutex.RLock()
	defer ctrl.twMutex.RUnlock()
	if len(ctrl.triggerWorkers) == 0 {
		return nil
	}
	//TODO 算法
	var key string
	c := math.MaxInt16
	for k, tWorker := range ctrl.triggerWorkers {
		curr := len(tWorker.subscriptions)
		if curr < c {
			key = k
		}
	}
	if key == "" {
		return nil
	}
	return ctrl.triggerWorkers[key]
}
