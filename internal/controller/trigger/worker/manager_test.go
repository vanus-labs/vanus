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
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	pbtrigger "github.com/linkall-labs/vsproto/pkg/trigger"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func getTestSubscription() *primitive.SubscriptionData {
	return &primitive.SubscriptionData{
		ID:    1,
		Phase: primitive.SubscriptionPhaseCreated,
	}
}

func getTestTriggerWorkerRemoveSubscription() OnTriggerWorkerRemoveSubscription {
	return func(ctx context.Context, subId vanus.ID, addr string) error {
		fmt.Println(fmt.Sprintf("trigger worker leave remove subscription %s", subId))
		return nil
	}
}

func getTestTriggerWorkerRemoveSubscriptionWithErr() OnTriggerWorkerRemoveSubscription {
	return func(ctx context.Context, subId vanus.ID, addr string) error {
		return fmt.Errorf("trigger worker leave remove subscription %s fail", subId)
	}
}

func TestInit(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	subManager := subscription.NewMockManager(ctrl)
	workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
	sub := getTestSubscription()
	sub.TriggerWorker = addr
	twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager, nil)
	Convey("test init", t, func() {
		workerStorage.EXPECT().ListTriggerWorker(ctx).Return([]*info.TriggerWorkerInfo{
			{Addr: addr},
		}, nil)
		subManager.EXPECT().ListSubscription(ctx).Return(map[vanus.ID]*primitive.SubscriptionData{
			sub.ID: sub,
		})
		err := twManager.Init(ctx)
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		So(tWorker, ShouldNotBeNil)
		subIds := tWorker.GetAssignSubIds()
		_, exist := subIds[sub.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestAddTriggerWorker(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
	twManager := NewTriggerWorkerManager(Config{}, workerStorage, nil, nil)
	Convey("test add", t, func() {
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		So(tWorker, ShouldNotBeNil)
		Convey("test repeat add", func() {
			err = twManager.AddTriggerWorker(ctx, addr)
			So(err, ShouldBeNil)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldNotBeNil)
		})
	})
}

func TestRemoveTriggerWorker(t *testing.T) {
	Convey("remove worker", t, func() {
		ctx := context.Background()
		addr := "test"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		sub := getTestSubscription()
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, nil, getTestTriggerWorkerRemoveSubscription())
		Convey("test remove not exist", func() {
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
			twManager.RemoveTriggerWorker(ctx, addr)
			tWorker := twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldBeNil)
		})
		Convey("test remove", func() {
			workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
			err := twManager.AddTriggerWorker(ctx, addr)
			So(err, ShouldBeNil)
			tWorker := twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldNotBeNil)
			tWorker.AddAssignSub(sub.ID)
			twManager.RemoveTriggerWorker(ctx, addr)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldBeNil)
		})

		Convey("test remove subscription error", func() {
			workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
			twManager = NewTriggerWorkerManager(Config{}, workerStorage, nil, getTestTriggerWorkerRemoveSubscriptionWithErr())
			err := twManager.AddTriggerWorker(ctx, addr)
			So(err, ShouldBeNil)
			tWorker := twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldNotBeNil)
			tWorker.AddAssignSub(sub.ID)
			twManager.RemoveTriggerWorker(ctx, addr)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldBeNil)
		})
	})
}

func TestAssignSubscription(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	sub := getTestSubscription()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	subManager := subscription.NewMockManager(ctrl)
	workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
	twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager, getTestTriggerWorkerRemoveSubscription())
	Convey("assign subscription", t, func() {
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		err = twManager.UpdateTriggerWorkerInfo(ctx, addr, map[vanus.ID]struct{}{})
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		err = tWorker.init(ctx)
		So(err, ShouldBeNil)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		tWorker.client = client
		client.EXPECT().AddSubscription(ctx, gomock.Any()).AnyTimes().Return(nil, nil)
		subManager.EXPECT().GetSubscription(ctx, sub.ID).Return(&primitive.Subscription{
			ID: sub.ID,
		}, nil)
		twManager.AssignSubscription(ctx, tWorker, sub.ID)
		So(len(tWorker.GetAssignSubIds()), ShouldEqual, 1)
		_, exist := tWorker.GetAssignSubIds()[sub.ID]
		So(exist, ShouldBeTrue)
	})
}

func TestUnAssignSubscription(t *testing.T) {
	ctx := context.Background()
	addr := "test"
	sub := getTestSubscription()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	subManager := subscription.NewMockManager(ctrl)
	workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
	twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager, getTestTriggerWorkerRemoveSubscription())
	Convey("unAssign subscription", t, func() {
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		err = tWorker.init(ctx)
		So(err, ShouldBeNil)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		tWorker.client = client
		err = twManager.UpdateTriggerWorkerInfo(ctx, addr, map[vanus.ID]struct{}{})
		So(err, ShouldBeNil)
		tWorker.AddAssignSub(sub.ID)
		So(len(tWorker.GetAssignSubIds()), ShouldEqual, 1)
		_, exist := tWorker.GetAssignSubIds()[sub.ID]
		So(exist, ShouldBeTrue)
		client.EXPECT().RemoveSubscription(ctx, gomock.Any()).AnyTimes().Return(nil, nil)
		err = twManager.UnAssignSubscription(ctx, addr, sub.ID)
		So(err, ShouldBeNil)
		So(len(tWorker.GetAssignSubIds()), ShouldEqual, 0)
	})
}

func TestPendingTriggerWorkerHandler(t *testing.T) {
	Convey("pending worker handler", t, func() {
		ctx := context.Background()
		addr := "test"
		sub := getTestSubscription()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subManager := subscription.NewMockManager(ctrl)
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager,
			getTestTriggerWorkerRemoveSubscription()).(*manager)
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		time.Sleep(time.Millisecond)
		Convey("pending worker start", func() {
			tWorker.PendingTime = time.Now().Add(twManager.config.StartWorkerDuration * -1)
			err = tWorker.init(ctx)
			So(err, ShouldBeNil)
			client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
			tWorker.client = client
			client.EXPECT().Start(ctx, gomock.Any()).Return(nil, nil)
			twManager.pendingTriggerWorkerHandler(ctx, tWorker)
		})
		Convey("pending worker clean", func() {
			tWorker.AddAssignSub(sub.ID)
			tWorker.PendingTime = time.Now().Add(twManager.config.WaitRunningTimeout * -1)
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).Return(nil)
			twManager.pendingTriggerWorkerHandler(ctx, tWorker)
			So(tWorker.GetPhase(), ShouldEqual, info.TriggerWorkerPhasePaused)
			So(len(tWorker.GetAssignSubIds()), ShouldEqual, 0)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldBeNil)
		})
	})
}

func TestRunningTriggerWorkerHandler(t *testing.T) {
	Convey("running worker handler", t, func() {
		ctx := context.Background()
		addr := "test"
		sub := getTestSubscription()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subManager := subscription.NewMockManager(ctrl)
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager,
			getTestTriggerWorkerRemoveSubscription()).(*manager)
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		err = twManager.UpdateTriggerWorkerInfo(ctx, addr, map[vanus.ID]struct{}{})
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		time.Sleep(time.Millisecond)
		Convey("running worker heartbeat timeout", func() {
			hbTime := time.Now().Add(twManager.config.HeartbeatTimeout * -1)
			tWorker.HeartbeatTime = &hbTime
			twManager.runningTriggerWorkerHandler(ctx, tWorker)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldNotBeNil)
			So(tWorker.GetPhase(), ShouldEqual, info.TriggerWorkerPhaseDisconnect)
		})

		Convey("running worker lost heartbeat ", func() {
			hbTime := time.Now().Add(twManager.config.LostHeartbeatTime * -1)
			tWorker.HeartbeatTime = &hbTime
			twManager.runningTriggerWorkerHandler(ctx, tWorker)
			tWorker = twManager.GetTriggerWorker(ctx, addr)
			So(tWorker, ShouldNotBeNil)
			So(tWorker.GetPhase(), ShouldEqual, info.TriggerWorkerPhaseRunning)
		})

		Convey("running worker start subscription", func() {
			hbTime := time.Now().Add(twManager.config.StartSubscriptionDuration * -1)
			tWorker.SetReportSubId(map[vanus.ID]struct{}{vanus.ID(2): {}})
			tWorker.AssignSubIds = map[vanus.ID]time.Time{sub.ID: hbTime}
			subManager.EXPECT().Heartbeat(ctx, gomock.Any(), addr, gomock.Any()).Return(nil)
			Convey("subscription not exist", func() {
				subManager.EXPECT().GetSubscription(ctx, sub.ID).Return(nil, subscription.ErrSubscriptionNotExist)
				twManager.runningTriggerWorkerHandler(ctx, tWorker)
				So(len(tWorker.GetAssignSubIds()), ShouldEqual, 0)
			})
			Convey("subscription  start", func() {
				subManager.EXPECT().GetSubscription(ctx, sub.ID).Return(&primitive.Subscription{
					ID: sub.ID,
				}, nil)
				err = tWorker.init(ctx)
				So(err, ShouldBeNil)
				client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
				tWorker.client = client
				client.EXPECT().AddSubscription(ctx, gomock.Any()).Return(nil, nil)
				twManager.runningTriggerWorkerHandler(ctx, tWorker)
			})
		})

	})

}

func TestManagerCheck(t *testing.T) {
	Convey("check", t, func() {
		ctx := context.Background()
		addr := "test"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subManager := subscription.NewMockManager(ctrl)
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager,
			getTestTriggerWorkerRemoveSubscription()).(*manager)
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		tWorker := twManager.GetTriggerWorker(ctx, addr)
		time.Sleep(time.Millisecond)
		Convey("pending check", func() {
			twManager.check(ctx)
		})
		Convey("running check", func() {
			tWorker.SetPhase(info.TriggerWorkerPhaseRunning)
			twManager.check(ctx)
		})
		Convey("disconnect check", func() {
			hbTime := time.Now().Add(twManager.config.DisconnectCleanTime * -1)
			tWorker.HeartbeatTime = &hbTime
			tWorker.SetPhase(info.TriggerWorkerPhaseDisconnect)
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).Return(nil)
			twManager.check(ctx)
		})
		Convey("pause check", func() {
			tWorker.SetPhase(info.TriggerWorkerPhasePaused)
			workerStorage.EXPECT().DeleteTriggerWorker(ctx, gomock.Any()).Return(nil)
			twManager.check(ctx)
		})
	})
}

func TestGetActiveWorker(t *testing.T) {
	Convey("get active worker", t, func() {
		ctx := context.Background()
		addr := "test"
		addr2 := "test2"
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subManager := subscription.NewMockManager(ctrl)
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager,
			getTestTriggerWorkerRemoveSubscription())
		workerStorage.EXPECT().SaveTriggerWorker(ctx, gomock.Any()).AnyTimes().Return(nil)
		err := twManager.AddTriggerWorker(ctx, addr)
		So(err, ShouldBeNil)
		err = twManager.UpdateTriggerWorkerInfo(ctx, addr, map[vanus.ID]struct{}{})
		So(err, ShouldBeNil)
		err = twManager.AddTriggerWorker(ctx, addr2)
		So(err, ShouldBeNil)
		time.Sleep(time.Millisecond)
		Convey("active worker", func() {
			tWorker1 := twManager.GetTriggerWorker(ctx, addr)
			So(tWorker1, ShouldNotBeNil)
			tWorker2 := twManager.GetTriggerWorker(ctx, addr2)
			So(tWorker2, ShouldNotBeNil)
			tWorkers := twManager.GetActiveRunningTriggerWorker()
			So(len(tWorkers), ShouldEqual, 1)
			So(tWorkers[0].Addr, ShouldEqual, tWorker1.info.Addr)
		})

	})

}

func TestManagerStartStop(t *testing.T) {
	Convey("manager start stop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subManager := subscription.NewMockManager(ctrl)
		workerStorage := storage.NewMockTriggerWorkerStorage(ctrl)
		twManager := NewTriggerWorkerManager(Config{}, workerStorage, subManager,
			getTestTriggerWorkerRemoveSubscription())
		twManager.Start()
		time.Sleep(time.Millisecond)
		twManager.Stop()
	})
}
