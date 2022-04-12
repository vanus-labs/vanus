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
	"errors"
	"github.com/linkall-labs/vanus/internal/controller/trigger/policy"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/observability/log"
)

var (
	notFoundTriggerWorker = errors.New("not found trigger worker")
)

type SubscriptionScheduler struct {
	normalQueue          queue.Queue
	maxRetryPrintLog     int
	policy               policy.TriggerWorkerPolicy
	triggerWorkerManager *TriggerWorkerManager
	subscriptionManager  *SubscriptionManager
	ctx                  context.Context
	stop                 context.CancelFunc
}

func NewSubscriptionScheduler(triggerWorkerManager *TriggerWorkerManager, subscriptionManager *SubscriptionManager) *SubscriptionScheduler {
	s := &SubscriptionScheduler{
		normalQueue:          queue.New(),
		maxRetryPrintLog:     5,
		policy:               &policy.RoundRobinPolicy{},
		triggerWorkerManager: triggerWorkerManager,
		subscriptionManager:  subscriptionManager,
	}
	s.ctx, s.stop = context.WithCancel(context.Background())
	return s
}

func (s *SubscriptionScheduler) EnqueueSub(subId string) {
	s.normalQueue.Add(subId)
}

func (s *SubscriptionScheduler) EnqueueNormalSub(subId string) {
	s.normalQueue.Add(subId)
}

func (s *SubscriptionScheduler) Stop() {
	s.stop()
	s.normalQueue.ShutDown()
}
func (s *SubscriptionScheduler) Run() {
	go func() {
		ctx := s.ctx
		for {
			subId, stop := s.normalQueue.Get()
			if stop {
				break
			}
			go func(subId string) {
				err := s.handler(ctx, subId)
				if err == nil {
					s.normalQueue.Done(subId)
					s.normalQueue.ClearFailNum(subId)
				} else {
					s.normalQueue.ReAdd(subId)
					log.Warning(ctx, "scheduler handler subscription has error", map[string]interface{}{
						log.KeyError:          err,
						log.KeySubscriptionID: subId,
					})
				}
			}(subId)
		}
	}()
}

func (s *SubscriptionScheduler) handler(ctx context.Context, subId string) error {
	twInfos := s.triggerWorkerManager.GetRunningTriggerWorker()
	if len(twInfos) == 0 {
		return notFoundTriggerWorker
	}
	twInfo := s.policy.Acquire(ctx, twInfos)
	subData := s.subscriptionManager.GetSubscription(ctx, subId)
	if subData == nil {
		return nil
	}
	if subData.Phase != primitive.SubscriptionPhaseCreated || subData.Phase != primitive.SubscriptionPhasePending {
		return nil
	}
	err := s.triggerWorkerManager.AddSubscription(ctx, twInfo, subId)
	if err != nil {
		return err
	}
	subData.TriggerWorker = twInfo.Addr
	subData.Phase = primitive.SubscriptionPhaseRunning
	err = s.subscriptionManager.UpdateSubscription(ctx, subData)
	if err != nil {
		return err
	}
	return nil
}
