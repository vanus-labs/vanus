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
	"errors"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriptionSchedulerHandler(t *testing.T) {
	Convey("test scheduler handler", t, func() {
		ctx := context.Background()
		subscriptionID := vanus.ID(1)
		workerAddr := "test"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tWorker := NewMockTriggerWorker(ctrl)
		subscriptionManager := subscription.NewMockManager(ctrl)
		workerManager := NewMockManager(ctrl)
		scheduler := NewSubscriptionScheduler(workerManager, subscriptionManager)

		Convey("test handler subscription is nil", func() {
			subscriptionManager.EXPECT().GetSubscription(ctx, subscriptionID).Return(nil)
			scheduler.handler(ctx, subscriptionID)
		})

		tWorker.EXPECT().AssignSubscription(gomock.Any()).AnyTimes().Return()
		Convey("test scheduler handler has trigger worker ", func() {
			subscriptionManager.EXPECT().GetSubscription(ctx, subscriptionID).Return(
				&metadata.Subscription{
					ID:            subscriptionID,
					Phase:         metadata.SubscriptionPhaseCreated,
					TriggerWorker: workerAddr,
				})
			workerManager.EXPECT().GetTriggerWorker(workerAddr).Return(tWorker)
			subscriptionManager.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
			scheduler.handler(ctx, subscriptionID)
		})

		Convey("test scheduler handler from no trigger worker ", func() {
			subscriptionManager.EXPECT().GetSubscription(ctx, subscriptionID).Return(
				&metadata.Subscription{
					ID:    subscriptionID,
					Phase: metadata.SubscriptionPhaseCreated,
				})
			workerManager.EXPECT().GetActiveRunningTriggerWorker().AnyTimes().Return([]metadata.TriggerWorkerInfo{
				{Addr: workerAddr},
			})
			workerManager.EXPECT().GetTriggerWorker(workerAddr).Return(tWorker)
			subscriptionManager.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
			scheduler.handler(ctx, subscriptionID)
		})
	})
}

func TestSubscriptionSchedulerRun(t *testing.T) {
	Convey("test scheduler run", t, func() {
		subscriptionID := vanus.ID(1)
		workerAddr := "test"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		workerManager := NewMockManager(ctrl)
		scheduler := NewSubscriptionScheduler(workerManager, subscriptionManager)
		Convey("test scheduler run handler no error", func() {
			subscriptionManager.EXPECT().GetSubscription(scheduler.ctx, gomock.Any()).Return(nil)
			scheduler.EnqueueNormalSubscription(subscriptionID)
			scheduler.Run()
			time.Sleep(10 * time.Millisecond)
			scheduler.Stop()
		})

		Convey("test scheduler run handler error", func() {
			ctx := scheduler.ctx
			subscriptionManager.EXPECT().GetSubscription(ctx, subscriptionID).Return(
				&metadata.Subscription{
					ID:            subscriptionID,
					TriggerWorker: workerAddr,
					Phase:         metadata.SubscriptionPhasePending,
				})
			workerManager.EXPECT().GetTriggerWorker(workerAddr).Return(NewTriggerWorkerByAddr(workerAddr, subscriptionManager))
			subscriptionManager.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(errors.New("update error"))
			scheduler.EnqueueSubscription(subscriptionID)
			scheduler.Run()
			time.Sleep(10 * time.Millisecond)
			scheduler.Stop()
		})
	})
}
