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
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	subscriptiontest "github.com/linkall-labs/vanus/internal/controller/trigger/subscription/testing"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/util"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func getTestSubscription() *primitive.SubscriptionApi {
	return &primitive.SubscriptionApi{
		ID: uuid.NewString(),
	}
}

func getTestTriggerWorkerRemoveSubscription() OnTriggerWorkerRemoveSubscription {
	return func(ctx context.Context, subId, addr string) error {
		fmt.Println(fmt.Sprintf("trigger worker leave remove subscription %s", subId))
		return nil
	}
}

func TestInit(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	storage := storage.NewFakeStorage()
	storage.SaveTriggerWorker(ctx, info.TriggerWorkerInfo{
		Id:   util.GetIdByAddr(addr),
		Addr: addr,
	})
	ctrl := gomock.NewController(t)
	subManager := subscriptiontest.NewMockManager(ctrl)
	sub := getTestSubscription()
	sub.TriggerWorker = addr
	subManager.EXPECT().ListSubscription(ctx).Return(map[string]*primitive.SubscriptionApi{
		sub.ID: sub,
	})
	twManager := NewTriggerWorkerManager(storage, subManager, getTestTriggerWorkerRemoveSubscription()).(*manager)
	twManager.Init(context.Background())
	Convey("test init", t, func() {
		So(len(twManager.triggerWorkers), ShouldEqual, 1)
		tWorker, exist := twManager.triggerWorkers[addr]
		So(exist, ShouldBeTrue)
		So(tWorker, ShouldNotBeNil)
		subIds := tWorker.GetAssignSubIds()
		_, exist = subIds[sub.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestAddTriggerWorker(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	storage := storage.NewFakeStorage()
	twManager := NewTriggerWorkerManager(storage, nil, getTestTriggerWorkerRemoveSubscription()).(*manager)

	Convey("test add", t, func() {
		twManager.AddTriggerWorker(ctx, addr)
		So(len(twManager.triggerWorkers), ShouldEqual, 1)
		tWorker, exist := twManager.triggerWorkers[addr]
		So(exist, ShouldBeTrue)
		So(tWorker, ShouldNotBeNil)
		Convey("test repeat add", func() {
			twManager.AddTriggerWorker(ctx, addr)
			So(len(twManager.triggerWorkers), ShouldEqual, 1)
			tWorker, exist = twManager.triggerWorkers[addr]
			So(exist, ShouldBeTrue)
			So(tWorker, ShouldNotBeNil)
		})
	})
}

func TestRemoveTriggerWorker(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	storage := storage.NewFakeStorage()
	twManager := NewTriggerWorkerManager(storage, nil, getTestTriggerWorkerRemoveSubscription()).(*manager)
	Convey("test remove not exist", t, func() {
		twManager.RemoveTriggerWorker(ctx, addr)
		So(len(twManager.triggerWorkers), ShouldEqual, 0)
		Convey("test remove", func() {
			tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo(addr))
			twManager.triggerWorkers[addr] = tWorker
			twManager.RemoveTriggerWorker(ctx, addr)
			So(len(tWorker.GetAssignSubIds()), ShouldEqual, 0)
			So(len(twManager.triggerWorkers), ShouldEqual, 0)
		})
	})
}
