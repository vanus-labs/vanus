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
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vsproto/pkg/trigger"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

//triggerWorker send Subscription to trigger worker
type triggerWorker struct {
	twAddr        string
	twCc          *grpc.ClientConn
	twClient      trigger.TriggerWorkerClient
	subscriptions map[string]*primitive.Subscription
	subLock       sync.Mutex
}

func NewTriggerWorker(addr string) *triggerWorker {
	return &triggerWorker{
		twAddr:        addr,
		subscriptions: map[string]*primitive.Subscription{},
	}
}

func (tw *triggerWorker) Close() error {
	return tw.twCc.Close()
}

func (tw *triggerWorker) Stop() error {
	err := tw.stopTriggerWorker()
	if err != nil {
		return errors.Wrapf(err, "stop trigger worker %s error", tw.twAddr)
	}
	return tw.Close()
}

func (tw *triggerWorker) Start(started bool) error {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	cc, err := grpc.Dial(tw.twAddr, opts...)
	if err != nil {
		return errors.Wrapf(err, "grpc dail %s error", tw.twAddr)
	}
	tw.twCc = cc
	tw.twClient = trigger.NewTriggerWorkerClient(cc)
	if started {
		return nil
	}
	err = tw.startTriggerWorker()
	if err != nil {
		tw.twCc.Close()
		return errors.Wrapf(err, "start trigger worker %s error", tw.twAddr)
	}
	return nil
}

func (tw *triggerWorker) startTriggerWorker() error {
	_, err := tw.twClient.Start(context.Background(), &trigger.StartTriggerWorkerRequest{})
	if err != nil {
		return errors.Wrap(err, "twClient start triggerWorker error")
	}
	return nil
}

func (tw *triggerWorker) stopTriggerWorker() error {
	_, err := tw.twClient.Stop(context.Background(), &trigger.StopTriggerWorkerRequest{})
	if err != nil {
		return errors.Wrap(err, "twClient stop triggerWorker error")
	}
	return nil
}

func (tw *triggerWorker) addSubscription(sub *primitive.Subscription) {
	tw.subLock.Lock()
	defer tw.subLock.Unlock()
	tw.subscriptions[sub.ID] = sub
}
func (tw *triggerWorker) AddSubscription(sub *primitive.Subscription) error {
	if sub == nil {
		return nil
	}
	tw.subLock.Lock()
	defer tw.subLock.Unlock()
	ctx := context.Background()
	to, err := convert.ToPbSubscription(sub)
	if err != nil {
		return errors.Wrap(err, "add subscription model convert error")
	}
	_, err = tw.twClient.AddSubscription(ctx, &trigger.AddSubscriptionRequest{
		Subscription: to,
	})
	if err != nil {
		return errors.Wrapf(err, "twClient %s add subscription %s error", tw.twClient, sub.ID)
	}
	tw.subscriptions[sub.ID] = sub
	return nil
}

func (tw *triggerWorker) RemoveSubscriptions(id string) error {
	tw.subLock.Lock()
	defer tw.subLock.Unlock()
	request := &trigger.RemoveSubscriptionRequest{Id: id}
	_, err := tw.twClient.RemoveSubscription(context.Background(), request)
	if err != nil {
		return errors.Wrapf(err, "twClient %s remove subscription %s error", tw.twAddr, id)
	}
	delete(tw.subscriptions, id)
	return nil
}

func (tw *triggerWorker) PauseSubscriptions(id string) error {
	tw.subLock.Lock()
	defer tw.subLock.Unlock()
	request := &trigger.PauseSubscriptionRequest{Id: id}
	_, err := tw.twClient.PauseSubscription(context.Background(), request)
	if err != nil {
		return errors.Wrapf(err, "twClient %s pause subscription %s error", tw.twAddr, id)
	}
	return nil
}
