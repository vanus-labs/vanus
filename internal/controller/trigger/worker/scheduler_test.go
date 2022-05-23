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

	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
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
		subscriptionManager := subscription.NewMockManager(ctrl)
		workerManager := NewMockManager(ctrl)
		scheduler := NewSubscriptionScheduler(workerManager, subscriptionManager)
		workerManager.EXPECT().GetActiveRunningTriggerWorker().AnyTimes().Return([]info.TriggerWorkerInfo{
			{Addr: workerAddr},
		})
		Convey("test handler subscription phase not match", func() {
			subscriptionManager.EXPECT().GetSubscriptionData(ctx, subscriptionID).Return(
				&primitive.SubscriptionData{
					ID:    subscriptionID,
					Phase: primitive.SubscriptionPhaseScheduled,
				})
			scheduler.handler(ctx, subscriptionID.String())
		})

		Convey("test scheduler handler normal ", func() {
			subscriptionManager.EXPECT().GetSubscriptionData(ctx, subscriptionID).Return(
				&primitive.SubscriptionData{
					ID:    subscriptionID,
					Phase: primitive.SubscriptionPhaseCreated,
				})
			workerManager.EXPECT().GetTriggerWorker(ctx, workerAddr).Return(NewTriggerWorkerByAddr(workerAddr))
			subscriptionManager.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
			workerManager.EXPECT().AssignSubscription(ctx, gomock.Any(), gomock.Any())
			scheduler.handler(ctx, subscriptionID.String())
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
		workerManager.EXPECT().GetActiveRunningTriggerWorker().AnyTimes().Return([]info.TriggerWorkerInfo{
			{Addr: workerAddr},
		})
		Convey("test scheduler run handler no error", func() {
			subscriptionManager.EXPECT().GetSubscriptionData(scheduler.ctx, gomock.Any()).Return(
				&primitive.SubscriptionData{
					ID:    subscriptionID,
					Phase: primitive.SubscriptionPhaseScheduled,
				})
			scheduler.EnqueueSub(subscriptionID)
			scheduler.Run()
			time.Sleep(10 * time.Millisecond)
			scheduler.Stop()
		})

		Convey("test scheduler run handler error", func() {
			ctx := scheduler.ctx
			subscriptionManager.EXPECT().GetSubscriptionData(ctx, subscriptionID).Return(
				&primitive.SubscriptionData{
					ID:    subscriptionID,
					Phase: primitive.SubscriptionPhaseCreated,
				})
			workerManager.EXPECT().GetTriggerWorker(ctx, workerAddr).Return(NewTriggerWorkerByAddr(workerAddr))
			subscriptionManager.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(errors.New("update error"))
			scheduler.EnqueueSub(subscriptionID)
			scheduler.Run()
			time.Sleep(10 * time.Millisecond)
			scheduler.Stop()
		})

	})
}
