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

//go:generate mockgen -source=worker.go  -destination=mock_worker.go -package=worker
package worker

import (
	"context"
	stdErr "errors"
	"fmt"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/proto/pkg/trigger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	errNoInit = fmt.Errorf("trigger worker not init")
)

type TriggerWorker interface {
	Init(ctx context.Context) error
	RemoteStart(ctx context.Context) error
	RemoteStop(ctx context.Context) error
	Close() error
	IsActive() bool
	Reset()
	GetInfo() info.TriggerWorkerInfo
	GetAddr() string
	SetPhase(info.TriggerWorkerPhase)
	GetPhase() info.TriggerWorkerPhase
	GetPendingTime() time.Time
	GetHeartbeatTime() time.Time
	ReportSubscription(ids []vanus.ID)
	AssignSubscription(id vanus.ID)
	UnAssignSubscription(id vanus.ID)
	GetAssignSubscriptions() []vanus.ID
}

// triggerWorker send subscription to trigger worker server.
type triggerWorker struct {
	info                  *info.TriggerWorkerInfo
	cc                    *grpc.ClientConn
	client                trigger.TriggerWorkerClient
	lock                  sync.RWMutex
	assignSubscriptionIDs map[vanus.ID]time.Time
	reportSubscriptionIDs map[vanus.ID]struct{}
	pendingTime           time.Time
	heartbeatTime         time.Time
	ctx                   context.Context
	stop                  context.CancelFunc
	subscriptionManager   subscription.Manager
	subscriptionQueue     queue.Queue
}

func NewTriggerWorkerByAddr(addr string, subscriptionManager subscription.Manager) TriggerWorker {
	tw := NewTriggerWorker(info.NewTriggerWorkerInfo(addr), subscriptionManager)
	return tw
}

func NewTriggerWorker(twInfo *info.TriggerWorkerInfo, subscriptionManager subscription.Manager) TriggerWorker {
	tw := &triggerWorker{
		info:                  twInfo,
		subscriptionManager:   subscriptionManager,
		subscriptionQueue:     queue.New(),
		pendingTime:           time.Now(),
		assignSubscriptionIDs: map[vanus.ID]time.Time{},
		reportSubscriptionIDs: map[vanus.ID]struct{}{},
	}
	tw.ctx, tw.stop = context.WithCancel(context.Background())
	go func() {
		ctx := tw.ctx
		for {
			subscriptionID, stop := tw.subscriptionQueue.Get()
			if stop {
				break
			}
			log.Info(ctx, "trigger worker begin hand subscription", map[string]interface{}{
				log.KeyTriggerWorkerAddr: tw.info.Addr,
				log.KeySubscriptionID:    subscriptionID,
			})
			err := tw.handler(ctx, subscriptionID)
			if err == nil {
				tw.subscriptionQueue.Done(subscriptionID)
				tw.subscriptionQueue.ClearFailNum(subscriptionID)
			} else {
				tw.subscriptionQueue.ReAdd(subscriptionID)
				log.Warning(ctx, "trigger worker handler subscription has error", map[string]interface{}{
					log.KeyError:             err,
					log.KeyTriggerWorkerAddr: tw.info.Addr,
					log.KeySubscriptionID:    subscriptionID,
				})
			}
		}
	}()
	return tw
}

func (tw *triggerWorker) handler(ctx context.Context, subscriptionID vanus.ID) error {
	tw.lock.RLock()
	_, exist := tw.assignSubscriptionIDs[subscriptionID]
	tw.lock.RUnlock()
	if !exist {
		//no assign to this trigger worker,remove subscription
		return tw.removeSubscription(ctx, subscriptionID)
	}
	return tw.addSubscription(ctx, subscriptionID)
}

func (tw *triggerWorker) IsActive() bool {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	if tw.info.Phase != info.TriggerWorkerPhaseRunning {
		return false
	}
	if tw.heartbeatTime.IsZero() {
		return false
	}
	return true
}

// Reset when trigger worker restart and re-connect.
func (tw *triggerWorker) Reset() {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.reportSubscriptionIDs = map[vanus.ID]struct{}{}
	tw.info.Phase = info.TriggerWorkerPhasePending
	tw.pendingTime = time.Now()
}

func (tw *triggerWorker) GetInfo() info.TriggerWorkerInfo {
	return *tw.info
}

func (tw *triggerWorker) GetAddr() string {
	return tw.info.Addr
}

func (tw *triggerWorker) SetPhase(phase info.TriggerWorkerPhase) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.info.Phase = phase
}

func (tw *triggerWorker) GetPhase() info.TriggerWorkerPhase {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	return tw.info.Phase
}

// ReportSubscription trigger worker running subscription ID.
func (tw *triggerWorker) ReportSubscription(ids []vanus.ID) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	for id := range tw.reportSubscriptionIDs {
		delete(tw.reportSubscriptionIDs, id)
	}
	now := time.Now()
	for _, id := range ids {
		tw.reportSubscriptionIDs[id] = struct{}{}
		_ = tw.subscriptionManager.Heartbeat(tw.ctx, id, tw.info.Addr, now)
	}
	tw.heartbeatTime = now
}

func (tw *triggerWorker) AssignSubscription(id vanus.ID) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	log.Info(context.Background(), "trigger worker assign a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tw.info.Addr,
		log.KeySubscriptionID:    id,
	})
	tw.assignSubscriptionIDs[id] = time.Now()
	tw.subscriptionQueue.Add(id)
}

func (tw *triggerWorker) UnAssignSubscription(id vanus.ID) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	log.Info(context.Background(), "trigger worker remove a subscription", map[string]interface{}{
		log.KeyTriggerWorkerAddr: tw.info.Addr,
		log.KeySubscriptionID:    id,
	})
	delete(tw.assignSubscriptionIDs, id)
	if tw.info.Phase == info.TriggerWorkerPhaseRunning {
		err := tw.removeSubscription(tw.ctx, id)
		if err != nil {
			log.Warning(context.Background(), "trigger worker remove subscription error", map[string]interface{}{
				log.KeyError:             err,
				log.KeyTriggerWorkerAddr: tw.info.Addr,
				log.KeySubscriptionID:    id,
			})
			tw.subscriptionQueue.Add(id)
		}
	}
}

func (tw *triggerWorker) GetAssignSubscriptions() []vanus.ID {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	var ids []vanus.ID
	for id := range tw.assignSubscriptionIDs {
		ids = append(ids, id)
	}
	return ids
}

func (tw *triggerWorker) GetPendingTime() time.Time {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	return tw.pendingTime
}

func (tw *triggerWorker) GetHeartbeatTime() time.Time {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	return tw.heartbeatTime
}

func (tw *triggerWorker) Init(ctx context.Context) error {
	if tw.cc != nil {
		return nil
	}
	var err error
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	tw.cc, err = grpc.DialContext(ctx, tw.info.Addr, opts...)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("grpc dial error").Wrap(err)
	}
	tw.client = trigger.NewTriggerWorkerClient(tw.cc)
	return nil
}

func (tw *triggerWorker) Close() error {
	if tw.cc != nil {
		tw.lock.Lock()
		defer tw.lock.Unlock()
		return tw.cc.Close()
	}
	tw.stop()
	tw.subscriptionQueue.ShutDown()
	return nil
}

func (tw *triggerWorker) RemoteStop(ctx context.Context) error {
	if tw.client == nil {
		return errNoInit
	}
	_, err := tw.client.Stop(ctx, &trigger.StopTriggerWorkerRequest{})
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("stop error").Wrap(err)
	}
	return nil
}

func (tw *triggerWorker) RemoteStart(ctx context.Context) error {
	if tw.client == nil {
		return errNoInit
	}
	_, err := tw.client.Start(ctx, &trigger.StartTriggerWorkerRequest{})
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("start error").Wrap(err)
	}
	return nil
}

func (tw *triggerWorker) addSubscription(ctx context.Context, id vanus.ID) error {
	if tw.client == nil {
		return errNoInit
	}
	sub, err := tw.subscriptionManager.GetSubscription(ctx, id)
	if err != nil {
		if stdErr.Is(err, subscription.ErrSubscriptionNotExist) {
			return nil
		}
		return err
	}
	request := convert.ToPbAddSubscription(sub)
	_, err = tw.client.AddSubscription(ctx, request)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("add subscription error").Wrap(err)
	}
	return nil
}

func (tw *triggerWorker) removeSubscription(ctx context.Context, id vanus.ID) error {
	if tw.client == nil {
		return errNoInit
	}
	request := &trigger.RemoveSubscriptionRequest{SubscriptionId: uint64(id)}
	_, err := tw.client.RemoveSubscription(ctx, request)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("remove subscription error").Wrap(err)
	}
	return nil
}
