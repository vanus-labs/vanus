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
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
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

var (
	serverNotReady     = errors.New("server not ready，is starting")
	triggerWorkerExist = errors.New("trigger worker exist")
)

type controllerState string

const (
	controllerInit     controllerState = "init"
	controllerStarting controllerState = "starting"
	controllerRunning  controllerState = "running"
	controllerStopping controllerState = "stopping"
	controllerStopped  controllerState = "stopped"
)

//triggerController allocate subscription to trigger processor
type triggerController struct {
	config               Config
	storage              storage.Storage
	heartbeats           map[string]time.Time
	triggerWorkers       map[string]*triggerWorker
	subscriptions        map[string]*subscriptionTrigger
	pendingSubscriptions map[string]*primitive.Subscription
	pendingSubMutex      sync.Mutex
	subMutex             sync.Mutex
	twMutex              sync.RWMutex
	stopCh               chan struct{}
	state                controllerState
}

func NewTriggerController(config Config) *triggerController {
	return &triggerController{
		heartbeats:           map[string]time.Time{},
		pendingSubscriptions: map[string]*primitive.Subscription{},
		triggerWorkers:       map[string]*triggerWorker{},
		subscriptions:        map[string]*subscriptionTrigger{},
		stopCh:               make(chan struct{}),
		config:               config,
		state:                controllerInit,
	}
}

func (ctrl *triggerController) CreateSubscription(ctx context.Context, request *ctrlpb.CreateSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != controllerRunning {
		return nil, serverNotReady
	}
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
	err = ctrl.storage.CreateSubscription(sub)
	if err != nil {
		return nil, errors.Wrap(err, "save subscription error")
	}
	err = ctrl.addSubToPending(sub)
	if err != nil {
		return nil, err
	}
	resp, err := convert.ToPbSubscription(sub)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
func (ctrl *triggerController) DeleteSubscription(ctx context.Context, request *ctrlpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	if ctrl.state != controllerRunning {
		return nil, serverNotReady
	}
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
	if ctrl.state != controllerRunning {
		return nil, serverNotReady
	}
	sub := ctrl.getSubscription(request.Id)
	resp, _ := convert.ToPbSubscription(sub)
	return resp, nil
}
func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	for {
		req, err := heartbeat.Recv()
		if err == nil {
			if ctrl.state == controllerStarting {
				if _, exist := ctrl.triggerWorkers[req.Address]; !exist {
					tWorker, err := ctrl.addTriggerWorker(req.Address, true)
					if err == nil {
						ctrl.pendingSubMutex.Lock()
						for _, id := range req.SubIds {
							if sub, exist := ctrl.pendingSubscriptions[id]; exist {
								tWorker.addSubscription(sub)
								delete(ctrl.pendingSubscriptions, id)
							} else {
								//不应该出现这样的情况
								log.Error("trigger worker has sub,but pending sub not exist", map[string]interface{}{
									"id": id,
								})
							}
						}
						ctrl.pendingSubMutex.Unlock()
					} else {
						if err != triggerWorkerExist {
							log.Error("heartbeat add trigger worker failed", map[string]interface{}{
								"addr":       req.Address,
								log.KeyError: err,
							})
							continue
						}
					}
				}
			}
			ctrl.heartbeats[req.Address] = time.Now()
		} else {
			if err == io.EOF {
				//client close,will remove trigger worker then receive unregister
				return nil
			}
			log.Info("heartbeat recv error", map[string]interface{}{log.KeyError: err})
			return err
		}
	}
}

func (ctrl *triggerController) RegisterTriggerWorker(ctx context.Context, request *ctrlpb.RegisterTriggerWorkerRequest) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	_, err := ctrl.addTriggerWorker(request.Address, false)
	if err != nil && err != triggerWorkerExist {
		log.Error("add trigger worker failed", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) UnregisterTriggerWorker(ctx context.Context, request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	ctrl.removeTriggerWorker(request.Address, "client api")
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) addSubToPending(subs ...*primitive.Subscription) error {
	ctrl.pendingSubMutex.Lock()
	ctrl.pendingSubMutex.Unlock()
	for _, sub := range subs {
		ctrl.pendingSubscriptions[sub.ID] = sub
	}
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
	ctrl.storage.DeleteOffset(id)
	ctrl.storage.DeleteSubscription(id)
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

func (ctrl *triggerController) addTriggerWorker(addr string, started bool) (*triggerWorker, error) {
	ctrl.twMutex.Lock()
	ctrl.twMutex.Unlock()
	if _, exist := ctrl.triggerWorkers[addr]; exist {
		log.Info("repeat add trigger worker,ignore", map[string]interface{}{
			"addr": addr,
		})
		return nil, triggerWorkerExist
	}
	tWorker := NewTriggerWorker(addr)
	err := tWorker.Start(started)
	if err != nil {
		return nil, errors.Wrapf(err, "trigger worker %s start error", addr)
	}
	ctrl.heartbeats[addr] = time.Now()
	ctrl.triggerWorkers[addr] = tWorker
	log.Info("add a trigger worker", map[string]interface{}{"twAddr": addr})
	return tWorker, nil
}

//removeTriggerWorker trigger worker has stop
func (ctrl *triggerController) removeTriggerWorker(addr, reason string) {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	tWorker, ok := ctrl.triggerWorkers[addr]
	if !ok {
		log.Info("remove trigger worker but not exist", map[string]interface{}{"" +
			"twAddr": addr,
		})
		return
	}
	//rejoin pending sub
	for _, sub := range tWorker.subscriptions {
		ctrl.addSubToPending(sub)
	}
	delete(ctrl.triggerWorkers, addr)
	delete(ctrl.heartbeats, addr)
	tWorker.Close()
	log.Info("remove a trigger worker", map[string]interface{}{"twAddr": addr, "reason": reason})
}

func (ctrl *triggerController) Start() error {
	s, err := storage.NewSubscriptionStorage(ctrl.config.Storage)
	if err != nil {
		return err
	}
	ctrl.storage = s

	//TODO page list
	subList, err := s.ListSubscription()
	if err != nil {
		s.Close()
		return errors.Wrap(err, "list subscription error")
	}
	log.Info("triggerController subscription size", map[string]interface{}{
		"size": len(subList),
	})
	for _, sub := range subList {
		ctrl.addSubToPending(sub)
	}
	ctrl.state = controllerStarting
	//wait triggerWorker heartbeat
	time.Sleep(time.Second * 10)
	ctrl.run()
	ctrl.state = controllerRunning
	return nil
}

func (ctrl *triggerController) Close() error {
	ctrl.state = controllerStopping
	close(ctrl.stopCh)
	ctrl.storage.Close()
	ctrl.state = controllerStopped
	return nil
}

func (ctrl *triggerController) run() {
	go func() {
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctrl.stopCh:
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
			case <-ctrl.stopCh:
				return
			case <-tk.C:
				now := time.Now()
				for twAddr := range ctrl.triggerWorkers {
					if last, exist := ctrl.heartbeats[twAddr]; exist {
						if now.Sub(last) < 5*time.Second {
							continue
						}
					}
					//remove trigger workers
					go func() {
						ctrl.removeTriggerWorker(twAddr, "heartbeat check")
					}()
				}
			}
		}
	}()
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
