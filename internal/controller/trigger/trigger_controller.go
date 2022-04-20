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
	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/controller/trigger/validation"
	"github.com/linkall-labs/vanus/internal/controller/trigger/worker"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
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
	member               embedetcd.Member
	storage              storage.Storage
	subscriptionManager  subscription.Manager
	triggerWorkerManager worker.Manager
	scheduler            *worker.SubscriptionScheduler
	needCleanSubIds      map[vanus.ID]string
	lock                 sync.Mutex
	membershipMutex      sync.Mutex
	isLeader             bool
	ctx                  context.Context
	stopFunc             context.CancelFunc
	state                primitive.ServerState
}

func NewTriggerController(config Config, member embedetcd.Member) *triggerController {
	ctrl := &triggerController{
		config:          config,
		member:          member,
		needCleanSubIds: map[vanus.ID]string{},
		state:           primitive.ServerStateCreated,
	}
	ctrl.ctx, ctrl.stopFunc = context.WithCancel(context.Background())
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
	subID := vanus.ID(request.Id)
	subData := ctrl.subscriptionManager.GetSubscriptionData(ctx, subID)
	if subData != nil {
		subData.Phase = primitive.SubscriptionPhaseToDelete
		err := ctrl.subscriptionManager.UpdateSubscription(ctx, subData)
		if err != nil {
			return nil, err
		}
		go func(subID vanus.ID, addr string) {
			err := ctrl.gcSubscription(ctrl.ctx, subID, addr)
			if err != nil {
				ctrl.lock.Lock()
				defer ctrl.lock.Unlock()
				ctrl.needCleanSubIds[subID] = addr
			}
		}(subID, subData.TriggerWorker)
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *triggerController) GetSubscription(ctx context.Context, request *ctrlpb.GetSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	sub := ctrl.subscriptionManager.GetSubscriptionData(ctx, vanus.ID(request.Id))
	if sub == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}

func (ctrl *triggerController) TriggerWorkerHeartbeat(heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	ctx := ctrl.ctx
	for {
		select {
		case <-ctx.Done():
			heartbeat.SendAndClose(&ctrlpb.TriggerWorkerHeartbeatResponse{})
			return nil
		default:
		}
		req, err := heartbeat.Recv()
		if err == nil {
			subIds := make(map[vanus.ID]struct{}, len(req.SubInfos))
			for _, sub := range req.SubInfos {
				subIds[vanus.ID(sub.SubscriptionId)] = struct{}{}
			}
			err = ctrl.triggerWorkerManager.UpdateTriggerWorkerInfo(ctx, req.Address, subIds)
			if err != nil {
				log.Info(context.Background(), "unknown trigger worker", map[string]interface{}{
					"addr": req.Address,
				})
				return errors.ErrResourceNotFound.WithMessage("unknown trigger worker")
			}
			for _, sub := range req.SubInfos {
				offsets := convert.FromPbOffsetInfos(sub.Offsets)
				err = ctrl.subscriptionManager.Offset(ctx, vanus.ID(sub.SubscriptionId), offsets)
				if err != nil {
					log.Warning(ctx, "heartbeat commit offset error", map[string]interface{}{
						log.KeyError:          err,
						log.KeySubscriptionID: sub.SubscriptionId,
					})
				}
			}
		} else {
			if err == io.EOF {
				//client close,will remove trigger worker then receive unregister
				return nil
			}
			log.Info(ctx, "heartbeat recv error", map[string]interface{}{log.KeyError: err})
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
	ctrl.triggerWorkerManager.RemoveTriggerWorker(ctrl.ctx, request.Address)
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, nil
}

//gcSubscription before delete subscription,need
//
//1.trigger worker remove subscription
//2.delete offset
//3.delete subscription
func (ctrl *triggerController) gcSubscription(ctx context.Context, subId vanus.ID, addr string) error {
	err := ctrl.triggerWorkerManager.UnAssignSubscription(ctx, addr, subId)
	if err != nil {
		return err
	}
	err = ctrl.subscriptionManager.DeleteSubscription(ctx, subId)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *triggerController) gcSubscriptions(ctx context.Context) {
	util.UntilWithContext(ctx, func(ctx context.Context) {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()
		for subId, addr := range ctrl.needCleanSubIds {
			err := ctrl.gcSubscription(ctx, subId, addr)
			if err == nil {
				delete(ctrl.needCleanSubIds, subId)
			}
		}
	}, time.Second*10)
}

func (ctrl *triggerController) requeueSubscription(ctx context.Context, subId vanus.ID, addr string) error {
	subData := ctrl.subscriptionManager.GetSubscriptionData(ctx, subId)
	if subData == nil {
		return nil
	}
	if subData.TriggerWorker != addr {
		//数据不一致了，不应该出现这种情况
		log.Error(ctx, "requeue subscription invalid", map[string]interface{}{
			log.KeyTriggerWorkerAddr: subData.TriggerWorker,
			"runningAddr":            addr,
		})
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
	ctrl.subscriptionManager = subscription.NewSubscriptionManager(ctrl.storage)
	ctrl.triggerWorkerManager = worker.NewTriggerWorkerManager(worker.Config{}, ctrl.storage, ctrl.subscriptionManager, ctrl.requeueSubscription)
	ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.triggerWorkerManager, ctrl.subscriptionManager)
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

func (ctrl *triggerController) membershipChangedProcessor(ctx context.Context, event embedetcd.MembershipChangedEvent) error {
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()
	switch event.Type {
	case embedetcd.EventBecomeLeader:
		if ctrl.isLeader {
			return nil
		}
		log.Info(ctrl.ctx, "become leader", nil)
		err := ctrl.init(ctx)
		if err != nil {
			ctrl.stop(ctx)
			return err
		}
		ctrl.triggerWorkerManager.Start()
		ctrl.subscriptionManager.Start()
		ctrl.scheduler.Run()
		go ctrl.gcSubscriptions(ctx)
		ctrl.state = primitive.ServerStateRunning
		ctrl.isLeader = true
	case embedetcd.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		log.Info(ctrl.ctx, "become flower", nil)
		ctrl.stop(ctx)
		ctrl.isLeader = false
	}
	return nil
}

func (ctrl *triggerController) stop(ctx context.Context) error {
	ctrl.member.ResignIfLeader(ctx)
	ctrl.state = primitive.ServerStateStopping
	ctrl.stopFunc()
	ctrl.scheduler.Stop()
	ctrl.triggerWorkerManager.Stop()
	ctrl.subscriptionManager.Stop()
	ctrl.storage.Close()
	ctrl.state = primitive.ServerStateStopped
	return nil
}

func (ctrl *triggerController) Start() error {
	ctx := ctrl.ctx
	s, err := storage.NewStorage(ctrl.config.Storage)
	if err != nil {
		return err
	}
	ctrl.storage = s
	go ctrl.member.RegisterMembershipChangedProcessor(ctx, ctrl.membershipChangedProcessor)
	return nil
}

func (ctrl *triggerController) Stop() error {
	ctrl.stop(ctrl.ctx)
	return nil
}
