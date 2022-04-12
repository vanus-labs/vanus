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
	"github.com/linkall-labs/vanus/internal/controller/errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

//TriggerWorker send SubscriptionApi to trigger worker server
type TriggerWorker struct {
	Info    *info.TriggerWorkerInfo
	cc      *grpc.ClientConn
	client  trigger.TriggerWorkerClient
	hasInit bool
	lock    sync.Mutex
}

func NewTriggerWorker(twInfo *info.TriggerWorkerInfo) *TriggerWorker {
	tw := &TriggerWorker{
		Info: twInfo,
	}
	return tw
}

func (tw *TriggerWorker) ResetReportSubId() {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.Info.ReportSubIds = map[string]struct{}{}
}

func (tw *TriggerWorker) SetReportSubId(subIds map[string]struct{}) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.Info.ReportSubIds = subIds
}

func (tw *TriggerWorker) GetReportSubId() map[string]struct{} {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	return tw.Info.ReportSubIds
}

func (tw *TriggerWorker) AddSub(subId string) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	tw.Info.SubIds[subId] = struct{}{}
}

func (tw *TriggerWorker) RemoveSub(subId string) {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	delete(tw.Info.SubIds, subId)
}

func (tw *TriggerWorker) GetSubIds() map[string]struct{} {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	return tw.Info.SubIds
}

func (tw *TriggerWorker) init(ctx context.Context) error {
	if tw.hasInit {
		return nil
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cc, err := grpc.DialContext(ctx, tw.Info.Addr, opts...)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("grpc dial error").Wrap(err)
	}
	tw.cc = cc
	tw.client = trigger.NewTriggerWorkerClient(cc)
	tw.hasInit = true
	return nil
}
func (tw *TriggerWorker) Close() error {
	if tw.cc != nil {
		tw.lock.Lock()
		defer tw.lock.Unlock()
		if !tw.hasInit {
			return nil
		}
		tw.hasInit = false
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

func (tw *TriggerWorker) RemoveSubscriptions(ctx context.Context, id string) error {
	tw.lock.Lock()
	defer tw.lock.Unlock()
	err := tw.init(ctx)
	if err != nil {
		return err
	}
	request := &trigger.RemoveSubscriptionRequest{Id: id}
	_, err = tw.client.RemoveSubscription(ctx, request)
	if err != nil {
		return errors.ErrTriggerWorker.WithMessage("remove subscription error").Wrap(err)
	}
	return nil
}
