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
	"fmt"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/trigger/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/offset"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	iInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"math"
	"sync"
	"time"
)

//triggerController allocate subscription to trigger processor
type triggerController struct {
	config           Config
	storage          storage.Storage
	triggerWorkers   map[string]*triggerWorker
	offsetManager    *offset.Manager
	subscriptions    map[string]string
	subQueue         queue.Queue
	maxRetryPrintLog int
	subMutex         sync.Mutex
	twMutex          sync.RWMutex
	ctx              context.Context
	stop             context.CancelFunc
	state            primitive.ServerState
}

func NewTriggerController(config Config) *triggerController {
	ctrl := &triggerController{
		subQueue:         queue.New(),
		maxRetryPrintLog: 5,
		triggerWorkers:   map[string]*triggerWorker{},
		subscriptions:    map[string]string{},
		config:           config,
		state:            primitive.ServerStateCreated,
	}
	ctrl.ctx, ctrl.stop = context.WithCancel(context.Background())
	return ctrl
}

func (ctrl *triggerController) CreateSubscription(ctx context.Context, request *ctrlpb.CreateSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	sub := convert.FromPbCreateSubscription(request)
	sub.ID = uuid.NewString()
	err := ctrl.storage.CreateSubscription(ctx, sub)
	if err != nil {
		return nil, err
	}
	ctrl.addSubToQueue(sub.ID, "api add a new sub")
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}
func (ctrl *triggerController) DeleteSubscription(ctx context.Context, request *ctrlpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	err := ctrl.deleteSubscription(ctx, request.Id)
	if err != nil {
		log.Error(ctx, "delete subscription failed", map[string]interface{}{
			"subId":      request.Id,
			log.KeyError: err,
		})
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (ctrl *triggerController) GetSubscription(ctx context.Context, request *ctrlpb.GetSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	sub, err := ctrl.getSubscription(request.Id)
	if err != nil {
		return nil, err
	}
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}
func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	for {
		req, err := heartbeat.Recv()
		if err == nil {
			tWorker := ctrl.getTriggerWorker(req.Address)
			if tWorker == nil {
				log.Info(context.Background(), "unknown trigger worker", map[string]interface{}{
					"addr": req.Address,
				})
				return errors.ErrResourceNotFound.WithMessage("unknown trigger worker")
			}
			tWorker.twInfo.Started = req.Started
			tWorker.twInfo.HeartbeatTime = time.Now()
			subInfos := make(map[string]*iInfo.SubscriptionInfo, len(req.SubInfos))
			for _, sub := range req.SubInfos {
				if ctrl.state == primitive.ServerStateStarted {
					ctrl.addSubscription(sub.SubscriptionId, req.Address)
				}
				subInfo := convert.FromPbSubscriptionInfo(sub)
				ctrl.offsetManager.Offset(*subInfo)
				subInfos[sub.SubscriptionId] = subInfo
			}
			tWorker.twInfo.SubInfos = subInfos
		} else {
			if err == io.EOF {
				//client close,will remove trigger worker then receive unregister
				return nil
			}
			log.Info(ctrl.ctx, "heartbeat recv error", map[string]interface{}{log.KeyError: err})
			return err
		}
	}
}

func (ctrl *triggerController) RegisterTriggerWorker(ctx context.Context, request *ctrlpb.RegisterTriggerWorkerRequest) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	err := ctrl.addTriggerWorker(info.NewTriggerWorkerInfo(request.Address))
	if err != nil {
		log.Error(ctx, "register new trigger worker failed", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) UnregisterTriggerWorker(ctx context.Context, request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	err := ctrl.removeTriggerWorker(ctx, request.Address, "client api")
	if err != nil {
		log.Info(ctx, "remove trigger worker error", map[string]interface{}{
			log.KeyError: err,
			"addr":       request.Address,
		})
	}
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, err
}

func (ctrl *triggerController) getTriggerWorker(addr string) *triggerWorker {
	ctrl.twMutex.RLock()
	defer ctrl.twMutex.RUnlock()
	return ctrl.triggerWorkers[addr]
}

func (ctrl *triggerController) deleteSubscription(ctx context.Context, id string) error {
	var err error
	defer func() {
		if err == nil {
			//clean storage
			ctrl.offsetManager.RemoveRegisterSubscription(id)
			err = ctrl.storage.DeleteOffset(ctx, id)
			err = ctrl.storage.DeleteSubscription(ctx, id)
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
			return err
		}
		delete(ctrl.subscriptions, id)
		log.Info(ctx, "delete subscription success", map[string]interface{}{"subId": id})
		return nil
	}

}

func (ctrl *triggerController) getSubscription(id string) (*primitive.Subscription, error) {
	return ctrl.storage.GetSubscription(context.Background(), id)
}

func (ctrl *triggerController) addSubscription(subId, addr string) {
	ctrl.subMutex.Lock()
	defer ctrl.subMutex.Unlock()
	ctrl.subscriptions[subId] = addr
}

func (ctrl *triggerController) addSubToQueue(subId, reason string) {
	ctrl.subQueue.Add(subId)
	log.Debug(ctrl.ctx, "add a sub to queue", map[string]interface{}{
		"subId":  subId,
		"reason": reason,
		"size":   ctrl.subQueue.Len(),
	})
}

func (ctrl *triggerController) addTriggerWorker(twInfo *info.TriggerWorkerInfo) error {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	addr := twInfo.Addr
	if tWorker, exist := ctrl.triggerWorkers[addr]; exist {
		log.Info(ctrl.ctx, "repeat add trigger worker", map[string]interface{}{
			"addr": addr,
			"now":  tWorker.twInfo,
			"new":  twInfo,
		})
		if tWorker.twInfo.Started && !twInfo.Started && len(tWorker.twInfo.SubInfos) > 0 {
			//triggerWorker restarting,reprocess sub
			for _, subInfo := range tWorker.twInfo.SubInfos {
				ctrl.addSubToQueue(subInfo.SubId, fmt.Sprintf("trigger worker restart %s", addr))
			}
		}
		tWorker.twInfo = twInfo
		return nil
	}
	tWorker, err := NewTriggerWorker(addr, twInfo)
	if err != nil {
		return err
	}
	err = ctrl.storage.SaveTriggerWorker(ctrl.ctx, *twInfo)
	if err != nil {
		return err
	}
	ctrl.triggerWorkers[addr] = tWorker
	log.Info(ctrl.ctx, "add a trigger worker", map[string]interface{}{"twAddr": addr})
	return nil
}

//removeTriggerWorker trigger worker has stop
func (ctrl *triggerController) removeTriggerWorker(ctx context.Context, addr, reason string) error {
	ctrl.twMutex.Lock()
	defer ctrl.twMutex.Unlock()
	tWorker, ok := ctrl.triggerWorkers[addr]
	if !ok {
		log.Info(ctx, "remove trigger worker but trigger worker not exist", map[string]interface{}{"" +
			"twAddr": addr,
		})
		return nil
	}
	err := ctrl.storage.DeleteTriggerWorker(ctx, info.GetIdByAddr(addr))
	if err != nil {
		return err
	}
	//rejoin pending sub
	for _, subInfo := range tWorker.twInfo.SubInfos {
		ctrl.addSubToQueue(subInfo.SubId, fmt.Sprintf("remove trigger worker %s", addr))
	}
	delete(ctrl.triggerWorkers, addr)
	tWorker.Close()
	log.Info(ctx, "remove a trigger worker", map[string]interface{}{"twAddr": addr, "reason": reason})
	return nil
}

func (ctrl *triggerController) initTriggerWorker() error {
	tWorkers, err := ctrl.storage.ListTriggerWorker(context.Background())
	if err != nil {
		return err
	}
	log.Info(ctrl.ctx, "trigger worker size", map[string]interface{}{"size": len(tWorkers)})
	for _, twInfo := range tWorkers {
		tWorker, err := NewTriggerWorker(twInfo.Addr, twInfo)
		if err != nil {
			return err
		}
		ctrl.triggerWorkers[twInfo.Addr] = tWorker
		log.Info(ctrl.ctx, "add a trigger worker", map[string]interface{}{"twAddr": twInfo.Addr})
	}
	return nil
}

func (ctrl *triggerController) Start() error {
	log.Info(ctrl.ctx, "trigger controller start...", nil)
	s, err := storage.NewStorage(ctrl.config.Storage)
	if err != nil {
		return err
	}
	ctrl.storage = s
	ctrl.offsetManager = offset.NewOffsetManager(s)
	ctrl.offsetManager.Start(ctrl.ctx)
	err = ctrl.initTriggerWorker()
	if err != nil {
		s.Close()
		return err
	}
	//TODO page list
	subList, err := s.ListSubscription(context.Background())
	if err != nil {
		s.Close()
		return err
	}
	log.Info(ctrl.ctx, "triggerController subscription size", map[string]interface{}{
		"size": len(subList),
	})
	go func() {
		log.Info(ctrl.ctx, "trigger controller init", nil)
		//wait all triggerWorker heartbeat,todo 优化
		time.Sleep(time.Second * time.Duration(ctrl.config.WaitTriggerTime))
		for _, sub := range subList {
			//left is no trigger worker heartbeat
			if _, exist := ctrl.subscriptions[sub.ID]; !exist {
				ctrl.addSubToQueue(sub.ID, "controller start")
			}
		}
		ctrl.run()
		ctrl.state = primitive.ServerStateRunning
		log.Info(ctrl.ctx, "trigger controller init complete", nil)
	}()
	ctrl.state = primitive.ServerStateStarted
	return nil
}

func (ctrl *triggerController) Close() error {
	ctrl.state = primitive.ServerStateStopping
	ctrl.stop()
	ctrl.subQueue.ShutDown()
	ctrl.storage.Close()
	ctrl.state = primitive.ServerStateStopped
	return nil
}

func (ctrl *triggerController) run() {
	go func() {
		for {
			if len(ctrl.triggerWorkers) == 0 {
				time.Sleep(time.Second)
				continue
			}
			subId, stop := ctrl.subQueue.Get()
			if stop {
				return
			}
			err := ctrl.processSubscription(subId)
			if err == nil {
				ctrl.subQueue.Done(subId)
				ctrl.subQueue.ClearFailNum(subId)
			} else {
				log.Debug(ctrl.ctx, "reAdd a sub to queue", map[string]interface{}{
					"subId": subId,
				})
				ctrl.subQueue.ReAdd(subId)
				if ctrl.subQueue.GetFailNum(subId)%ctrl.maxRetryPrintLog == 0 {
					log.Error(ctrl.ctx, "process subscription error", map[string]interface{}{
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
			case <-ctrl.ctx.Done():
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
					ctrl.removeTriggerWorker(ctrl.ctx, addr, "heartbeat check timeout")
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
					log.Info(ctrl.ctx, "start trigger worker has error", map[string]interface{}{
						"addr":       addr,
						log.KeyError: err,
					})
				} else {
					log.Info(ctrl.ctx, "start trigger worker success", map[string]interface{}{
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
				return errors.ErrFindTriggerWorkerTimeout
			}
			if findTime%10 == 0 {
				log.Debug(ctrl.ctx, "process subscriptions no found trigger processor", nil)
			}
			time.Sleep(time.Second * 5)
			continue
		}
		break
	}
	offsets, err := ctrl.offsetManager.GetOffset(ctrl.ctx, subId)
	if err != nil {
		return err
	}
	err = tWorker.AddSubscription(&primitive.SubscriptionInfo{Subscription: *sub, Offsets: offsets})
	if err != nil {
		return err
	}
	log.Info(ctrl.ctx, "allocate a sub to triggerWorker", map[string]interface{}{
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
		curr := len(tWorker.twInfo.SubInfos)
		if curr < c {
			tw = ctrl.triggerWorkers[addr]
		}
	}
	if tw == nil {
		return nil
	}
	return tw
}
