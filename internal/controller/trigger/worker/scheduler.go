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
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultRetryPrintLog = 5
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

func NewSubscriptionScheduler(triggerWorkerManager Manager,
	subscriptionManager subscription.Manager) *SubscriptionScheduler {
	s := &SubscriptionScheduler{
		normalQueue:          queue.New(),
		maxRetryPrintLog:     defaultRetryPrintLog,
		policy:               &RoundRobinPolicy{},
		triggerWorkerManager: triggerWorkerManager,
		subscriptionManager:  subscriptionManager,
	}
	s.ctx, s.stop = context.WithCancel(context.Background())
	return s
}

func (s *SubscriptionScheduler) EnqueueSubscription(id vanus.ID) {
	s.normalQueue.Add(id.String())
}

func (s *SubscriptionScheduler) EnqueueNormalSubscription(id vanus.ID) {
	s.normalQueue.Add(id.String())
}

func (s *SubscriptionScheduler) Stop() {
	s.stop()
	s.normalQueue.ShutDown()
}
func (s *SubscriptionScheduler) Run() {
	go func() {
		ctx := s.ctx
		for {
			subscriptionID, stop := s.normalQueue.Get()
			if stop {
				break
			}
			err := s.handler(ctx, subscriptionID)
			if err == nil {
				s.normalQueue.Done(subscriptionID)
				s.normalQueue.ClearFailNum(subscriptionID)
			} else {
				s.normalQueue.ReAdd(subscriptionID)
				log.Warning(ctx, "scheduler handler subscription has error", map[string]interface{}{
					log.KeyError:          err,
					log.KeySubscriptionID: subscriptionID,
				})
			}
		}
	}()
}

func (s *SubscriptionScheduler) handler(ctx context.Context, subscriptionIDStr string) error {
	subscriptionID, _ := vanus.NewIDFromString(subscriptionIDStr)
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
		subData := s.subscriptionManager.GetSubscriptionData(ctx, subscriptionID)
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
		s.triggerWorkerManager.AssignSubscription(ctx, tWorker, subscriptionID)
		return nil
	}
}
