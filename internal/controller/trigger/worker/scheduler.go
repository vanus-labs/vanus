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
	"github.com/linkall-labs/vanus/internal/controller/trigger/cache"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/policy"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/queue"
	"github.com/linkall-labs/vanus/observability/log"
	"time"
)

type SubscriptionScheduler struct {
	quickQueue        queue.Queue
	normalQueue       queue.Queue
	workerSize        int
	maxRetryPrintLog  int
	policy            policy.TriggerWorkerPolicy
	twManager         *TriggerWorkerManager
	subscriptionCache *cache.SubscriptionCache
}

func NewSubscriptionScheduler(twManager *TriggerWorkerManager, subscriptionCache *cache.SubscriptionCache) *SubscriptionScheduler {
	return &SubscriptionScheduler{
		quickQueue:        queue.New(),
		normalQueue:       queue.New(),
		workerSize:        2,
		maxRetryPrintLog:  5,
		policy:            &policy.RoundRobinPolicy{},
		twManager:         twManager,
		subscriptionCache: subscriptionCache,
	}
}

func (s *SubscriptionScheduler) EnqueueSub(subId string) {
	s.quickQueue.Add(subId)
}

func (s *SubscriptionScheduler) EnqueueNormalSub(subId string) {
	s.normalQueue.Add(subId)
}

func (s *SubscriptionScheduler) Run(ctx context.Context) {
	go func() {
		for {
			subId, stop := s.quickQueue.Get()
			if stop {
				break
			}
			go func(subId string) {
				defer func() {
					s.quickQueue.Done(subId)
					s.quickQueue.ClearFailNum(subId)
				}()
				err := s.handler(ctx, subId)
				if err != nil {
					log.Warning(ctx, "quick queue handler has error", map[string]interface{}{
						log.KeyError:          err,
						log.KeySubscriptionID: subId,
					})
					s.normalQueue.Add(subId)
				}
			}(subId)
		}
	}()
	for i := 0; i < s.workerSize; i++ {
		go func() {
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
					if s.normalQueue.GetFailNum(subId) > s.maxRetryPrintLog {
						log.Warning(ctx, "slow queue handler has error", map[string]interface{}{
							log.KeyError:          err,
							log.KeySubscriptionID: subId,
						})
					}
				}
			}
		}()
	}
}

func (s *SubscriptionScheduler) handler(ctx context.Context, subId string) error {
	var twInfo info.TriggerWorkerInfo
	begin := time.Now()
	for {
		twInfos := s.twManager.GetRunningTriggerWorker()
		if len(twInfos) == 0 {
			if time.Now().Sub(begin) > time.Minute {
				return errors.ErrFindTriggerWorkerTimeout
			}
			time.Sleep(time.Second)
		} else {
			break
		}
		twInfo = s.policy.Acquire(ctx, twInfos)
	}
	subData, err := s.subscriptionCache.GetSubscription(ctx, subId)
	if err != nil {
		if err == errors.ErrResourceNotFound {
			return nil
		}
		return err
	}
	if subData.Phase != primitive.SubscriptionPhaseCreated || subData.Phase != primitive.SubscriptionPhasePending {
		return nil
	}
	subData.TriggerWorker = twInfo.Addr
	err = s.twManager.AddSubscription(ctx, twInfo, subId)
	if err != nil {
		return err
	}
	subData.Phase = primitive.SubscriptionPhaseRunning
	err = s.subscriptionCache.UpdateSubscription(ctx, subData)
	if err != nil {
		return err
	}
	return nil
}
