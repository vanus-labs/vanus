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

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
)

const (
	defaultRetryPrintLog = 5
)

type SubscriptionScheduler struct {
	normalQueue         queue.Queue
	maxRetryPrintLog    int
	policy              TriggerWorkerPolicy
	workerManager       Manager
	subscriptionManager subscription.Manager
	ctx                 context.Context
	stop                context.CancelFunc
}

func NewSubscriptionScheduler(workerManager Manager,
	subscriptionManager subscription.Manager) *SubscriptionScheduler {
	s := &SubscriptionScheduler{
		normalQueue:         queue.New(),
		maxRetryPrintLog:    defaultRetryPrintLog,
		policy:              &RoundRobinPolicy{},
		workerManager:       workerManager,
		subscriptionManager: subscriptionManager,
	}
	s.ctx, s.stop = context.WithCancel(context.Background())
	return s
}

func (s *SubscriptionScheduler) EnqueueSubscription(id vanus.ID) {
	s.normalQueue.Add(id)
}

func (s *SubscriptionScheduler) EnqueueNormalSubscription(id vanus.ID) {
	s.normalQueue.Add(id)
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

func (s *SubscriptionScheduler) handler(ctx context.Context, subscriptionID vanus.ID) error {
	subscription := s.subscriptionManager.GetSubscription(ctx, subscriptionID)
	if subscription == nil {
		return nil
	}
	twAddr := subscription.TriggerWorker
	if twAddr == "" {
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			twInfos := s.workerManager.GetActiveRunningTriggerWorker()
			if len(twInfos) == 0 {
				time.Sleep(time.Second)
				continue
			}
			twInfo := s.policy.Acquire(ctx, twInfos)
			twAddr = twInfo.Addr
			break
		}
	}
	tWorker := s.workerManager.GetTriggerWorker(twAddr)
	if tWorker == nil {
		return ErrTriggerWorkerNotFound
	}
	if subscription.TriggerWorker == "" {
		metrics.CtrlTriggerGauge.WithLabelValues(twAddr).Inc()
	}
	subscription.TriggerWorker = twAddr
	subscription.Phase = metadata.SubscriptionPhaseScheduled
	subscription.HeartbeatTime = time.Now()
	err := s.subscriptionManager.UpdateSubscription(ctx, subscription)
	if err != nil {
		return err
	}
	tWorker.AssignSubscription(subscriptionID)
	return nil
}
