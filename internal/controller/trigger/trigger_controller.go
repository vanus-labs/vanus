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
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/validation"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"sync"
	"time"
)

//triggerController allocate subscription to trigger worker
type triggerController struct {
	config               Config
	storage              storage.Storage
	subscriptionManager  *SubscriptionManager
	triggerWorkerManager *TriggerWorkerManager
	scheduler            *SubscriptionScheduler
	needCleanSubIds      map[string]string
	lock                 sync.Mutex
	ctx                  context.Context
	stop                 context.CancelFunc
	state                primitive.ServerState
}

func NewTriggerController(config Config) *triggerController {
	ctrl := &triggerController{
		config:          config,
		needCleanSubIds: map[string]string{},
		state:           primitive.ServerStateCreated,
	}
	ctrl.ctx, ctrl.stop = context.WithCancel(context.Background())
	return ctrl
}

//CreateSubscription api storage
func (ctrl *triggerController) CreateSubscription(ctx context.Context, request *ctrlpb.CreateSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	err := validation.ConvertCreateSubscriptionRequest(request).Validate(ctx)
	if err != nil {
		return nil, err
	}
	sub := convert.FromPbCreateSubscription(request)
	err = ctrl.subscriptionManager.AddSubscription(ctx, sub)
	if err != nil {
		return nil, err
	}
	ctrl.scheduler.EnqueueNormalSub(sub.ID)
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}

func (ctrl *triggerController) DeleteSubscription(ctx context.Context, request *ctrlpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	subData := ctrl.subscriptionManager.GetSubscription(ctx, request.Id)
	if subData != nil {
		subData.Phase = primitive.SubscriptionPhaseToDelete
		err := ctrl.subscriptionManager.UpdateSubscription(ctx, subData)
		if err != nil {
			return nil, err
		}
		go func(subId, addr string) {
			err := ctrl.gcSubscription(ctx, subId, addr)
			if err != nil {
				ctrl.lock.Lock()
				defer ctrl.lock.Unlock()
				ctrl.needCleanSubIds[subId] = addr
			}
		}(request.Id, subData.TriggerWorker)
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *triggerController) GetSubscription(ctx context.Context, request *ctrlpb.GetSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	sub := ctrl.subscriptionManager.GetSubscription(ctx, request.Id)
	if sub == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}

func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	for {
		select {
		case <-ctrl.ctx.Done():
			heartbeat.SendAndClose(&ctrlpb.TriggerWorkerHeartbeatResponse{})
			return nil
		default:
		}
		req, err := heartbeat.Recv()
		if err == nil {
			subIds := make(map[string]struct{}, len(req.SubInfos))
			for _, sub := range req.SubInfos {
				subIds[sub.SubscriptionId] = struct{}{}
			}
			exist := ctrl.triggerWorkerManager.UpdateTriggerWorkerInfo(ctrl.ctx, req.Address, subIds)
			if !exist {
				log.Info(context.Background(), "unknown trigger worker", map[string]interface{}{
					"addr": req.Address,
				})
				return errors.ErrResourceNotFound.WithMessage("unknown trigger worker")
			}
			for _, sub := range req.SubInfos {
				subInfo := convert.FromPbSubscriptionInfo(sub)
				err = ctrl.subscriptionManager.Offset(ctrl.ctx, *subInfo)
				if err != nil {
					log.Warning(ctrl.ctx, "heartbeat commit offset error", map[string]interface{}{
						log.KeyError:          err,
						log.KeySubscriptionID: subInfo.SubId,
					})
				}
			}
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
	err := ctrl.triggerWorkerManager.AddTriggerWorker(ctx, request.Address)
	if err != nil {
		log.Warning(ctx, "register trigger worker error", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *triggerController) UnregisterTriggerWorker(ctx context.Context, request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	err := ctrl.triggerWorkerManager.RemoveTriggerWorker(ctx, request.Address)
	if err != nil {
		log.Error(ctx, "unregister trigger worker error", map[string]interface{}{
			log.KeyError: err,
			"addr":       request.Address,
		})
	}
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, err
}

//gcSubscription before delete subscription,need
//
//1.trigger worker remove subscription
//2.delete offset
//3.delete subscription
func (ctrl *triggerController) gcSubscription(ctx context.Context, subId, addr string) error {
	err := ctrl.triggerWorkerManager.RemoveSubscription(ctx, addr, subId)
	if err != nil {
		return err
	}
	err = ctrl.subscriptionManager.DeleteSubscription(ctx, subId)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *triggerController) triggerWorkerRemoveSubscription(ctx context.Context, subId, addr string) error {
	subData := ctrl.subscriptionManager.GetSubscription(ctx, subId)
	if subData == nil {
		return nil
	}
	if subData.TriggerWorker == "" || subData.TriggerWorker != addr {
		//数据不一致了，不应该出现这种情况
		return nil
	}
	subData.TriggerWorker = ""
	subData.Phase = primitive.SubscriptionPhasePending
	err := ctrl.subscriptionManager.UpdateSubscription(ctx, subData)
	if err != nil {
		return err
	}
	ctrl.scheduler.EnqueueSub(subId)
	return nil
}

func (ctrl *triggerController) init(ctx context.Context) error {
	ctrl.subscriptionManager = NewSubscriptionManager(ctrl.storage)
	ctrl.triggerWorkerManager = NewTriggerWorkerManager(ctrl.storage, ctrl.subscriptionManager, ctrl.triggerWorkerRemoveSubscription)
	ctrl.scheduler = NewSubscriptionScheduler(ctrl.triggerWorkerManager, ctrl.subscriptionManager)
	err := ctrl.subscriptionManager.Init(ctx)
	if err != nil {
		return err
	}
	err = ctrl.triggerWorkerManager.Init(ctx)
	if err != nil {
		return err
	}
	//restart,need reschedule
	for subId, sub := range ctrl.subscriptionManager.ListSubscription(ctx) {
		switch sub.Phase {
		case primitive.SubscriptionPhaseCreated:
			ctrl.scheduler.EnqueueNormalSub(subId)
		case primitive.SubscriptionPhasePending:
			ctrl.scheduler.EnqueueSub(subId)
		case primitive.SubscriptionPhaseToDelete:
			ctrl.needCleanSubIds[subId] = sub.TriggerWorker
		}
	}
	return nil
}

func (ctrl *triggerController) Start() error {
	ctx := ctrl.ctx
	log.Info(ctrl.ctx, "trigger controller start...", nil)
	s, err := storage.NewStorage(ctrl.config.Storage)
	if err != nil {
		return err
	}
	ctrl.storage = s
	err = ctrl.init(ctx)
	if err != nil {
		ctrl.storage.Close()
		return err
	}
	go func() {
		ctrl.triggerWorkerManager.Run(ctx)
		ctrl.subscriptionManager.Run()
		ctrl.scheduler.Run()
	}()

	go util.UntilWithContext(ctx, func(ctx context.Context) {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()
		for subId, addr := range ctrl.needCleanSubIds {
			err := ctrl.gcSubscription(ctx, subId, addr)
			if err != nil {
				delete(ctrl.needCleanSubIds, subId)
			}
		}
	}, time.Second*10)
	ctrl.state = primitive.ServerStateRunning
	return nil
}

func (ctrl *triggerController) Close() error {
	ctrl.state = primitive.ServerStateStopping
	ctrl.stop()
	ctrl.scheduler.Stop()
	ctrl.subscriptionManager.Stop()
	ctrl.storage.Close()
	ctrl.state = primitive.ServerStateStopped
	return nil
}
