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
	"fmt"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
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
	config           Config
	storage          storage.Storage
	triggerWorkers   map[string]*triggerWorker
	subscriptions    map[string]string
	subQueue         queue.Queue
	maxRetryPrintLog int
	subMutex         sync.Mutex
	twMutex          sync.RWMutex
	stopCh           chan struct{}
	state            controllerState
}

func NewTriggerController(config Config) *triggerController {
	return &triggerController{
		subQueue:         queue.New(),
		maxRetryPrintLog: 5,
		triggerWorkers:   map[string]*triggerWorker{},
		subscriptions:    map[string]string{},
		stopCh:           make(chan struct{}),
		config:           config,
		state:            controllerInit,
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
	ctrl.addSubToQueue(sub.ID, "api add a new sub")
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
	sub, err := ctrl.getSubscription(request.Id)
	if err != nil {
		return nil, err
	}
	resp, _ := convert.ToPbSubscription(sub)
	return resp, nil
}
func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	for {
		req, err := heartbeat.Recv()
		if err == nil {
			tWorker := ctrl.getTriggerWorker(req.Address)
			if tWorker == nil {
				tWorker, err = NewTriggerWorker(req.Address, &info.TriggerWorkerInfo{
					Addr:          req.Address,
					Started:       req.Started,
					SubIds:        req.SubIds,
					HeartbeatTime: time.Now(),
				})
				if err != nil {
					log.Error("heartbeat new trigger worker failed", map[string]interface{}{
						"addr":       req.Address,
						log.KeyError: err,
					})
				}
				ctrl.addTriggerWorker(tWorker)
				if len(tWorker.twInfo.SubIds) > 0 {
					for _, subId := range tWorker.twInfo.SubIds {
						log.Debug("heartbeat set sub addr", map[string]interface{}{
							"subId": subId,
							"addr":  req.Address,
						})
						ctrl.addSubscription(subId, req.Address)
					}
				}
			}
			tWorker.twInfo.HeartbeatTime = time.Now()
			tWorker.twInfo.SubIds = req.SubIds
			tWorker.twInfo.Started = req.Started
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
	tWorker, err := NewTriggerWorker(request.Address, &info.TriggerWorkerInfo{
		Addr:    request.Address,
		Started: false,
	})
	if err != nil {
		log.Error("register new trigger worker failed", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	ctrl.addTriggerWorker(tWorker)
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) UnregisterTriggerWorker(ctx context.Context, request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	ctrl.removeTriggerWorker(request.Address, "client api")
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) getTriggerWorker(addr string) *triggerWorker {
	ctrl.twMutex.RLock()
	defer ctrl.twMutex.RUnlock()
	return ctrl.triggerWorkers[addr]
}

func (ctrl *triggerController) deleteSubscription(id string) error {
	var err error
	defer func() {
		if err == nil {
			//clean storage
			err = ctrl.storage.DeleteOffset(id)
			err = ctrl.storage.DeleteSubscription(id)
		}
	}()
	ctrl.subMutex.Lock()
	defer ctrl.subMutex.Unlock()
	twAddr, ok := ctrl.subscriptions[id]
	if !ok {
		return nil
	}

	if tWorker := ctrl.getTriggerWorker(twAddr); tWorker == nil {
		return nil
	} else {
		err = tWorker.RemoveSubscriptions(id)
		if err != nil {
			err = errors.Wrapf(err, "trigger worker remove sub %s error", id)
			return err
		}
		delete(ctrl.subscriptions, id)
		log.Info("delete subscription success", map[string]interface{}{"subId": id})
		return nil
	}

}

func (ctrl *triggerController) getSubscription(id string) (*primitive.Subscription, error) {
	return ctrl.storage.GetSubscription(id)
}

func (ctrl *triggerController) addSubscription(subId, addr string) {
	ctrl.subMutex.Lock()
	defer ctrl.subMutex.Unlock()
	ctrl.subscriptions[subId] = addr
}

func (ctrl *triggerController) addSubToQueue(subId, reason string) {
	ctrl.subQueue.Add(subId)
	log.Debug("add a sub to queue", map[string]interface{}{
		"subId":  subId,
		"reason": reason,
		"size":   ctrl.subQueue.Len(),
	})
}

func (ctrl *triggerController) addTriggerWorker(newWorker *triggerWorker) {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	if tw, exist := ctrl.triggerWorkers[newWorker.twAddr]; exist {
		log.Info("repeat add trigger worker", map[string]interface{}{
			"addr":    newWorker.twAddr,
			"started": map[string]bool{"now": tw.twInfo.Started, "new": newWorker.twInfo.Started},
		})
		if tw.twInfo.Started && !newWorker.twInfo.Started {
			//restarting,rejoin pending sub
			for sub := range tw.subs {
				ctrl.addSubToQueue(sub, fmt.Sprintf("add trigger worker %s", newWorker.twAddr))
			}
		}
		tw.Close()
	}
	ctrl.triggerWorkers[newWorker.twAddr] = newWorker
	log.Info("add a trigger worker", map[string]interface{}{"twAddr": newWorker.twAddr})
}

//removeTriggerWorker trigger worker has stop
func (ctrl *triggerController) removeTriggerWorker(addr, reason string) {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	tWorker, ok := ctrl.triggerWorkers[addr]
	if !ok {
		log.Info("remove trigger worker but trigger worker not exist", map[string]interface{}{"" +
			"twAddr": addr,
		})
		return
	}
	//rejoin pending sub
	for _, subId := range tWorker.twInfo.SubIds {
		ctrl.addSubToQueue(subId, fmt.Sprintf("remove trigger worker %s", addr))
	}
	delete(ctrl.triggerWorkers, addr)
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
	ctrl.state = controllerStarting
	//wait all triggerWorker heartbeat,todo 优化
	time.Sleep(time.Second * 10)
	for _, sub := range subList {
		//left is no trigger worker heartbeat
		if _, exist := ctrl.subscriptions[sub.ID]; !exist {
			ctrl.addSubToQueue(sub.ID, "controller start")
		}
	}
	ctrl.run()
	ctrl.state = controllerRunning
	return nil
}

func (ctrl *triggerController) Close() error {
	ctrl.state = controllerStopping
	close(ctrl.stopCh)
	ctrl.subQueue.ShutDown()
	ctrl.storage.Close()
	ctrl.state = controllerStopped
	return nil
}

func (ctrl *triggerController) run() {
	go func() {
		for {
			subId, stop := ctrl.subQueue.Get()
			if stop {
				return
			}
			err := ctrl.processSubscription(subId)
			if err == nil {
				ctrl.subQueue.Done(subId)
				ctrl.subQueue.ClearFailNum(subId)
			} else {
				log.Debug("reAdd a sub to queue", map[string]interface{}{
					"subId": subId,
				})
				ctrl.subQueue.ReAdd(subId)
				if ctrl.subQueue.GetFailNum(subId)%ctrl.maxRetryPrintLog == 0 {
					log.Error("process subscription error", map[string]interface{}{
						"subId":      subId,
						log.KeyError: err,
					})
				}
			}
		}
	}()
	go func() {
		tk := time.NewTicker(5 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctrl.stopCh:
				return
			case <-tk.C:
				ctrl.checkTriggerWorker()
			}
		}
	}()
}
func (ctrl *triggerController) checkTriggerWorker() {
	ctrl.twMutex.RLock()
	defer ctrl.twMutex.RUnlock()
	if len(ctrl.triggerWorkers) == 0 {
		return
	}
	now := time.Now()
	for addr, tWorker := range ctrl.triggerWorkers {
		if tWorker.twInfo.Started {
			if now.Sub(tWorker.twInfo.HeartbeatTime) > 30*time.Second {
				//no heartbeat
				go func() {
					ctrl.removeTriggerWorker(addr, "heartbeat check timeout")
				}()
			}
			continue
		}
		if !tWorker.twInfo.IsStarting {
			tWorker.twInfo.IsStarting = true
			go func() {
				defer func() {
					tWorker.twInfo.IsStarting = false
				}()
				err := tWorker.Start()
				if err != nil {
					log.Info("start trigger worker has error", map[string]interface{}{
						"addr":       addr,
						log.KeyError: err,
					})
				} else {
					log.Info("start trigger worker success", map[string]interface{}{
						"addr": addr,
					})
				}
			}()

		}
	}
}
func (ctrl *triggerController) processSubscription(subId string) error {
	sub, err := ctrl.getSubscription(subId)
	if err != nil {
		return err
	}
	var tWorker *triggerWorker
	findTime := 0
	beginTime := time.Now()
	for {
		findTime++
		tWorker = ctrl.findTriggerWorker()
		if tWorker == nil {
			if ctrl.subQueue.IsShutDown() {
				return nil
			}
			if time.Now().Sub(beginTime) > 2*time.Minute {
				return errors.New("find trigger timeout")
			}
			if findTime%10 == 0 {
				log.Debug("process subscriptions no found trigger processor", nil)
			}
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}
	err = tWorker.AddSubscription(sub)
	if err != nil {
		return errors.Wrap(err, "tWorker add subscription error")
	}
	log.Info("allocate a sub to triggerWorker", map[string]interface{}{
		"subId":  subId,
		"twAddr": tWorker.twAddr,
	})
	ctrl.addSubscription(subId, tWorker.twAddr)
	return nil
}

func (ctrl *triggerController) findTriggerWorker() *triggerWorker {
	ctrl.twMutex.RLock()
	defer ctrl.twMutex.RUnlock()
	if len(ctrl.triggerWorkers) == 0 {
		return nil
	}
	//TODO 算法
	var tw *triggerWorker
	c := math.MaxInt16
	for addr, tWorker := range ctrl.triggerWorkers {
		if !tWorker.twInfo.Started {
			continue
		}
		curr := len(tWorker.twInfo.SubIds)
		if curr < c {
			tw = ctrl.triggerWorkers[addr]
		}
	}
	if tw == nil {
		return nil
	}
	return tw
}
