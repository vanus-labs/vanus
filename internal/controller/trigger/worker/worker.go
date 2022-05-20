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

package worker

import (
	"context"
	"sync"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vsproto/pkg/trigger"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//TriggerWorker send SubscriptionData to trigger worker server
type TriggerWorker struct {
	info   *info.TriggerWorkerInfo
	cc     *grpc.ClientConn
	client trigger.TriggerWorkerClient
	lock   sync.RWMutex
}

func NewTriggerWorker(twInfo *info.TriggerWorkerInfo) *TriggerWorker {
	tw := &TriggerWorker{
		info: twInfo,
	}
	return tw
}

//ResetReportSubId trigger worker restart
func (tw *TriggerWorker) ResetReportSubId() {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.info.ReportSubIds = map[vanus.ID]struct{}{}
	tw.info.Phase = info.TriggerWorkerPhasePending
	tw.info.PendingTime = time.Now()
}

//SetReportSubId trigger worker heartbeat running subscription
func (tw *TriggerWorker) SetReportSubId(subIds map[vanus.ID]struct{}) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.info.ReportSubIds = subIds
	now := time.Now()
	tw.info.HeartbeatTime = &now
}

func (tw *TriggerWorker) GetReportSubId() map[vanus.ID]struct{} {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	return tw.info.ReportSubIds
}

func (tw *TriggerWorker) AddAssignSub(subId vanus.ID) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.info.AssignSubIds[subId] = time.Now()
}

func (tw *TriggerWorker) RemoveAssignSub(subId vanus.ID) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	delete(tw.info.AssignSubIds, subId)
}

func (tw *TriggerWorker) GetAssignSubIds() map[vanus.ID]time.Time {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	newMap := make(map[vanus.ID]time.Time, len(tw.info.AssignSubIds))
	for subId, t := range tw.info.AssignSubIds {
		newMap[subId] = t
	}
	return newMap
}

func (tw *TriggerWorker) GetLastHeartbeatTime() time.Time {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	if tw.info.HeartbeatTime != nil {
		return *tw.info.HeartbeatTime
	}
	return tw.info.PendingTime
}

func (tw *TriggerWorker) SetPhase(phase info.TriggerWorkerPhase) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.info.Phase = phase
}

func (tw *TriggerWorker) GetPhase() info.TriggerWorkerPhase {
	tw.lock.RLock()
	defer tw.lock.RUnlock()
	return tw.info.Phase
}

func (tw *TriggerWorker) init(ctx context.Context) error {
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
func (tw *TriggerWorker) Close() error {
	if tw.cc != nil {
		tw.lock.Lock()
		defer tw.lock.Unlock()
		return tw.cc.Close()
	}
	return nil
}

func (tw *TriggerWorker) Stop(ctx context.Context) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	err := tw.init(ctx)
	if err != nil {
		return err
	}
	_, err = tw.client.Stop(ctx, &trigger.StopTriggerWorkerRequest{})
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("stop error").Wrap(err)
	}
	return nil
}

func (tw *TriggerWorker) Start(ctx context.Context) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	err := tw.init(ctx)
	if err != nil {
		return err
	}
	_, err = tw.client.Start(ctx, &trigger.StartTriggerWorkerRequest{})
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("start error").Wrap(err)
	}
	return nil
}

func (tw *TriggerWorker) AddSubscription(ctx context.Context, sub *primitive.Subscription) error {
	if sub == nil {
		return nil
	}
	tw.lock.Lock()
	defer tw.lock.Unlock()
	err := tw.init(ctx)
	if err != nil {
		return err
	}
	request := convert.ToPbAddSubscription(sub)
	_, err = tw.client.AddSubscription(ctx, request)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("add subscription error").Wrap(err)
	}
	return nil
}

func (tw *TriggerWorker) RemoveSubscriptions(ctx context.Context, id vanus.ID) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	err := tw.init(ctx)
	if err != nil {
		return err
	}
	request := &trigger.RemoveSubscriptionRequest{SubscriptionId: uint64(id)}
	_, err = tw.client.RemoveSubscription(ctx, request)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("remove subscription error").Wrap(err)
	}
	return nil
}
