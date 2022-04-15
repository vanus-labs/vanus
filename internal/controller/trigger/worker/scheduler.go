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
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"time"
)

type SubscriptionScheduler struct {
	normalQueue          queue.Queue
	maxRetryPrintLog     int
	policy               TriggerWorkerPolicy
	triggerWorkerManager Manager
	subscriptionManager  subscription.Manager
	ctx                  context.Context
	stop                 context.CancelFunc
}

func NewSubscriptionScheduler(triggerWorkerManager Manager, subscriptionManager subscription.Manager) *SubscriptionScheduler {
	s := &SubscriptionScheduler{
		normalQueue:          queue.New(),
		maxRetryPrintLog:     5,
		policy:               &TriggerSizePolicy{},
		triggerWorkerManager: triggerWorkerManager,
		subscriptionManager:  subscriptionManager,
	}
	s.ctx, s.stop = context.WithCancel(context.Background())
	return s
}

func (s *SubscriptionScheduler) EnqueueSub(subId vanus.ID) {
	s.normalQueue.Add(subId.String())
}

func (s *SubscriptionScheduler) EnqueueNormalSub(subId vanus.ID) {
	s.normalQueue.Add(subId.String())
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
		}
	}()
}

func (s *SubscriptionScheduler) handler(ctx context.Context, subIdStr string) error {
	subID, _ := vanus.StringToID(subIdStr)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		twInfos := s.triggerWorkerManager.GetActiveRunningTriggerWorker()
		if len(twInfos) == 0 {
			time.Sleep(time.Second)
			continue
		}
		twInfo := s.policy.Acquire(ctx, twInfos)
		subData := s.subscriptionManager.GetSubscriptionData(ctx, subID)
		if subData == nil {
			return nil
		}
		if subData.Phase != primitive.SubscriptionPhaseCreated && subData.Phase != primitive.SubscriptionPhasePending {
			return nil
		}
		tWorker := s.triggerWorkerManager.GetTriggerWorker(ctx, twInfo.Addr)
		if tWorker == nil {
			continue
		}
		subData.TriggerWorker = twInfo.Addr
		subData.Phase = primitive.SubscriptionPhaseScheduled
		subData.HeartbeatTime = time.Now()
		err := s.subscriptionManager.UpdateSubscription(ctx, subData)
		if err != nil {
			return err
		}
		s.triggerWorkerManager.AssignSubscription(ctx, tWorker, subID)
		return nil
	}
}
