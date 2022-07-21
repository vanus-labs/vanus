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
	stdErr "errors"
	"io"
	"reflect"
	"sync"
	"time"

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
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ ctrlpb.TriggerControllerServer = &controller{}
)

const (
	defaultGcSubscriptionPeriod = time.Second * 10
)

func NewController(config Config, member embedetcd.Member) *controller {
	ctrl := &controller{
		config:                config,
		member:                member,
		needCleanSubscription: map[vanus.ID]string{},
		state:                 primitive.ServerStateCreated,
	}
	ctrl.ctx, ctrl.stopFunc = context.WithCancel(context.Background())
	return ctrl
}

type controller struct {
	config                Config
	member                embedetcd.Member
	storage               storage.Storage
	subscriptionManager   subscription.Manager
	workerManager         worker.Manager
	scheduler             *worker.SubscriptionScheduler
	needCleanSubscription map[vanus.ID]string
	lock                  sync.Mutex
	membershipMutex       sync.Mutex
	isLeader              bool
	ctx                   context.Context
	stopFunc              context.CancelFunc
	state                 primitive.ServerState
}

func (ctrl *controller) CreateSubscription(ctx context.Context,
	request *ctrlpb.CreateSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	err := validation.ValidateCreateSubscription(ctx, request)
	if err != nil {
		return nil, err
	}
	sub := convert.FromPbCreateSubscription(request)
	err = ctrl.subscriptionManager.AddSubscription(ctx, sub)
	if err != nil {
		return nil, err
	}
	ctrl.scheduler.EnqueueNormalSubscription(sub.ID)
	resp := convert.ToPbSubscription(sub)
	return resp, nil
}

func (ctrl *controller) UpdateSubscription(ctx context.Context,
	request *ctrlpb.UpdateSubscriptionRequest) (*meta.Subscription, error) {
	if ctrl.state != primitive.ServerStateRunning {
		return nil, errors.ErrServerNotStart
	}
	subID := vanus.ID(request.Id)
	subscriptionData := ctrl.subscriptionManager.GetSubscriptionData(ctx, subID)
	if subscriptionData == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("subscription not exist")
	}
	err := validation.ValidateUpdateSubscription(ctx, request)
	if err != nil {
		return nil, err
	}
	change := false
	if request.Config != nil {
		// check rateLimit change
		config := convert.FromPbSubscriptionConfig(request.Config)
		c := subscriptionData.Config.Change(config)
		if !change {
			change = c
		}
	}
	if request.Filters != nil {
		filters := convert.FromPbFilters(request.Filters)
		if reflect.DeepEqual(filters, subscriptionData.Filters) {
			change = true
			subscriptionData.Filters = filters
		}
	}
	if request.Sink != "" {
		if string(subscriptionData.Sink) != request.Sink {
			change = true
			subscriptionData.Sink = primitive.URI(request.Sink)
		}
	}
	if request.InputTransformer != nil {
		transformer := convert.FromFPbInputTransformer(request.InputTransformer)
		if reflect.DeepEqual(transformer, subscriptionData.InputTransformer) {
			change = true
			subscriptionData.InputTransformer = transformer
		}
	}
	if !change {
		return nil, errors.ErrInvalidRequest.WithMessage("no change")
	}
	subscriptionData.Phase = primitive.SubscriptionPhasePending
	err = ctrl.subscriptionManager.UpdateSubscription(ctx, subscriptionData)
	if err != nil {
		return nil, err
	}
	ctrl.scheduler.EnqueueNormalSubscription(subscriptionData.ID)
	resp := convert.ToPbSubscription(subscriptionData)
	return resp, nil
}

func (ctrl *controller) DeleteSubscription(ctx context.Context,
	request *ctrlpb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
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
				ctrl.needCleanSubscription[subID] = addr
			}
		}(subID, subData.TriggerWorker)
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetSubscription(ctx context.Context,
	request *ctrlpb.GetSubscriptionRequest) (*meta.Subscription, error) {
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

func (ctrl *controller) TriggerWorkerHeartbeat(
	heartbeat ctrlpb.TriggerController_TriggerWorkerHeartbeatServer) error {
	ctx := ctrl.ctx
	for {
		select {
		case <-ctx.Done():
			heartbeat.SendAndClose(&ctrlpb.TriggerWorkerHeartbeatResponse{})
			return nil
		default:
		}
		if !ctrl.member.IsLeader() {
			heartbeat.SendAndClose(&ctrlpb.TriggerWorkerHeartbeatResponse{})
			return nil
		}
		req, err := heartbeat.Recv()
		if err != nil {
			if !stdErr.Is(err, io.EOF) {
				log.Warning(ctx, "heartbeat recv error", map[string]interface{}{log.KeyError: err})
			}
			log.Info(ctx, "heartbeat close", nil)
			return nil
		}
		log.Debug(ctx, "heartbeat", map[string]interface{}{
			log.KeyTriggerWorkerAddr: req.Address,
			"subscriptionInfo":       req.SubscriptionInfo,
		})
		var ids []vanus.ID
		for _, subInfo := range req.SubscriptionInfo {
			ids = append(ids, vanus.ID(subInfo.SubscriptionId))
		}
		err = ctrl.workerManager.UpdateTriggerWorkerInfo(ctx, req.Address, ids)
		if err != nil {
			log.Info(context.Background(), "unknown trigger worker", map[string]interface{}{
				log.KeyTriggerWorkerAddr: req.Address,
			})
			return errors.ErrResourceNotFound.WithMessage("unknown trigger worker")
		}
		for _, subInfo := range req.SubscriptionInfo {
			offsets := convert.FromPbOffsetInfos(subInfo.Offsets)
			err = ctrl.subscriptionManager.Offset(ctx, vanus.ID(subInfo.SubscriptionId), offsets)
			if err != nil {
				log.Warning(ctx, "heartbeat commit offset error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: subInfo.SubscriptionId,
				})
			}
		}
	}
}

func (ctrl *controller) RegisterTriggerWorker(ctx context.Context,
	request *ctrlpb.RegisterTriggerWorkerRequest) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	log.Info(ctx, "register trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: request.Address,
	})
	err := ctrl.workerManager.AddTriggerWorker(ctx, request.Address)
	if err != nil {
		log.Warning(ctx, "register trigger worker error", map[string]interface{}{
			"addr":       request.Address,
			log.KeyError: err,
		})
		return nil, err
	}
	return &ctrlpb.RegisterTriggerWorkerResponse{}, nil
}

func (ctrl *controller) UnregisterTriggerWorker(ctx context.Context,
	request *ctrlpb.UnregisterTriggerWorkerRequest) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	log.Info(ctx, "unregister trigger worker", map[string]interface{}{
		log.KeyTriggerWorkerAddr: request.Address,
	})

	ctrl.workerManager.RemoveTriggerWorker(context.TODO(), request.Address)
	return &ctrlpb.UnregisterTriggerWorkerResponse{}, nil
}

func (ctrl *controller) ListSubscription(ctx context.Context,
	_ *emptypb.Empty) (*ctrlpb.ListSubscriptionResponse, error) {
	subscriptions := ctrl.subscriptionManager.ListSubscription(ctx)
	list := make([]*meta.Subscription, 0, len(subscriptions))
	for _, sub := range subscriptions {
		list = append(list, convert.ToPbSubscription(sub))
	}
	return &ctrlpb.ListSubscriptionResponse{Subscription: list}, nil
}

// gcSubscription before delete subscription,need
//
// 1.trigger worker remove subscription
// 2.delete offset
// 3.delete subscription .
func (ctrl *controller) gcSubscription(ctx context.Context, id vanus.ID, addr string) error {
	tWorker := ctrl.workerManager.GetTriggerWorker(addr)
	if tWorker != nil {
		tWorker.UnAssignSubscription(id)
	}
	err := ctrl.subscriptionManager.DeleteSubscription(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *controller) gcSubscriptions(ctx context.Context) {
	util.UntilWithContext(ctx, func(ctx context.Context) {
		ctrl.lock.Lock()
		defer ctrl.lock.Unlock()
		for ID, addr := range ctrl.needCleanSubscription {
			err := ctrl.gcSubscription(ctx, ID, addr)
			if err == nil {
				delete(ctrl.needCleanSubscription, ID)
			}
		}
	}, defaultGcSubscriptionPeriod)
}

func (ctrl *controller) requeueSubscription(ctx context.Context, id vanus.ID, addr string) error {
	subData := ctrl.subscriptionManager.GetSubscriptionData(ctx, id)
	if subData == nil {
		return nil
	}
	if subData.TriggerWorker != addr {
		// 数据不一致了，不应该出现这种情况
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
	ctrl.scheduler.EnqueueSubscription(id)
	return nil
}

func (ctrl *controller) init(ctx context.Context) error {
	ctrl.subscriptionManager = subscription.NewSubscriptionManager(ctrl.storage)
	ctrl.workerManager = worker.NewTriggerWorkerManager(worker.Config{}, ctrl.storage,
		ctrl.subscriptionManager, ctrl.requeueSubscription)
	ctrl.scheduler = worker.NewSubscriptionScheduler(ctrl.workerManager, ctrl.subscriptionManager)
	err := ctrl.subscriptionManager.Init(ctx)
	if err != nil {
		return err
	}
	err = ctrl.workerManager.Init(ctx)
	if err != nil {
		return err
	}
	// restart,need reschedule
	for _, subscription := range ctrl.subscriptionManager.ListSubscription(ctx) {
		switch subscription.Phase {
		case primitive.SubscriptionPhaseCreated:
			ctrl.scheduler.EnqueueNormalSubscription(subscription.ID)
		case primitive.SubscriptionPhasePending:
			ctrl.scheduler.EnqueueSubscription(subscription.ID)
		case primitive.SubscriptionPhaseToDelete:
			ctrl.needCleanSubscription[subscription.ID] = subscription.TriggerWorker
		}
	}
	return nil
}

func (ctrl *controller) membershipChangedProcessor(ctx context.Context,
	event embedetcd.MembershipChangedEvent) error {
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()
	switch event.Type {
	case embedetcd.EventBecomeLeader:
		if ctrl.isLeader {
			return nil
		}
		log.Info(context.TODO(), "become leader", nil)
		err := ctrl.init(ctx)
		if err != nil {
			_err := ctrl.stop(ctx)
			if _err != nil {
				log.Error(ctx, "controller stop has error", map[string]interface{}{
					log.KeyError: _err,
				})
			}
			return err
		}
		ctrl.workerManager.Start()
		ctrl.subscriptionManager.Start()
		ctrl.scheduler.Run()
		go ctrl.gcSubscriptions(ctx)
		ctrl.state = primitive.ServerStateRunning
		ctrl.isLeader = true
	case embedetcd.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		log.Info(context.TODO(), "become flower", nil)
		_err := ctrl.stop(ctx)
		if _err != nil {
			log.Error(ctx, "controller stop has error", map[string]interface{}{
				log.KeyError: _err,
			})
		}
	}
	return nil
}

func (ctrl *controller) stop(ctx context.Context) error {
	ctrl.member.ResignIfLeader()
	ctrl.state = primitive.ServerStateStopping
	ctrl.stopFunc()
	ctrl.scheduler.Stop()
	ctrl.workerManager.Stop()
	ctrl.subscriptionManager.Stop()
	ctrl.storage.Close()
	ctrl.state = primitive.ServerStateStopped
	return nil
}

func (ctrl *controller) Start() error {
	s, err := storage.NewStorage(ctrl.config.Storage)
	if err != nil {
		return err
	}
	ctrl.storage = s
	go ctrl.member.RegisterMembershipChangedProcessor(ctrl.membershipChangedProcessor)
	return nil
}

func (ctrl *controller) Stop(ctx context.Context) {
	if err := ctrl.stop(ctx); err != nil {
		log.Warning(ctx, "stop trigger controller error", map[string]interface{}{
			log.KeyError: err,
		})
	}
}
