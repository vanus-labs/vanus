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

	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTriggerWorker_ResetOffsetToTimestamp(t *testing.T) {
	Convey("test reset offset to timestamp", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		tWorker.client = client
		id := vanus.NewID()
		client.EXPECT().ResetOffsetToTimestamp(gomock.Any(), gomock.Any()).Return(nil, nil)
		err := tWorker.ResetOffsetToTimestamp(id, uint64(time.Now().Unix()))
		So(err, ShouldBeNil)
		_ = tWorker.Close()
	})
}

func TestTriggerWorker_AssignSubscription(t *testing.T) {
	Convey("test assign subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		addr := "test"
		subscriptionManager.EXPECT().GetSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 0)
		id := vanus.NewID()
		tWorker.AssignSubscription(id)
		So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 1)
		_, exist := tWorker.assignSubscriptionIDs.Load(id)
		So(exist, ShouldBeTrue)
		id2 := vanus.NewID()
		tWorker.AssignSubscription(id2)
		So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 2)
		_, exist = tWorker.assignSubscriptionIDs.Load(id2)
		So(exist, ShouldBeTrue)
		_ = tWorker.Close()
	})
}

func TestTriggerWorker_UnAssignSubscription(t *testing.T) {
	Convey("test unAssign subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		tWorker.client = client
		tWorker.SetPhase(metadata.TriggerWorkerPhaseRunning)
		Convey("remove subscription no error", func() {
			id := vanus.NewID()
			tWorker.assignSubscriptionIDs.Store(id, time.Now())
			So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 1)
			client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(nil, nil)
			tWorker.UnAssignSubscription(id)
			So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 0)
		})
		Convey("remove subscription has error", func() {
			id := vanus.NewID()
			tWorker.assignSubscriptionIDs.Store(id, time.Now())
			So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 1)
			client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, fmt.Errorf("error"))
			tWorker.UnAssignSubscription(id)
			So(len(tWorker.GetAssignedSubscriptions()), ShouldEqual, 0)
		})
		_ = tWorker.Close()
	})
}

func TestTriggerWorker_QueueHandler(t *testing.T) {
	Convey("test trigger worker queue subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		err := tWorker.Start(context.Background())
		So(err, ShouldBeNil)
		tWorker.client = client
		tWorker.SetPhase(metadata.TriggerWorkerPhaseRunning)
		client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(nil, nil)
		tWorker.subscriptionQueue.Add(2)
		time.Sleep(time.Millisecond * 100)
		client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, fmt.Errorf("error"))
		tWorker.subscriptionQueue.Add(1)
		_ = tWorker.Close()
	})
}

func TestTriggerWorker_Handler(t *testing.T) {
	Convey("test trigger worker handler", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		tWorker.client = client
		Convey("remove subscription", func() {
			id := vanus.NewID()
			client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(nil, nil)
			err := tWorker.handler(ctx, id)
			So(err, ShouldBeNil)
			client.EXPECT().RemoveSubscription(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("error"))
			err = tWorker.handler(ctx, id)
			So(err, ShouldNotBeNil)
		})
		Convey("add subscription", func() {
			id := vanus.NewID()
			tWorker.assignSubscriptionIDs.Store(id, time.Now())
			sub := &metadata.Subscription{
				ID:     id,
				Source: "test-source",
				Types:  []string{"type-1", "type-2"},
				Filters: []*primitive.SubscriptionFilter{
					{Exact: map[string]string{"k": "v"}},
				},
			}
			subscriptionManager.EXPECT().GetSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(sub)
			subscriptionManager.EXPECT().GetOffset(gomock.Any(), gomock.Any()).AnyTimes().Return(info.ListOffsetInfo{}, nil)
			client.EXPECT().AddSubscription(gomock.Any(), gomock.Any()).Return(nil, nil)
			subscriptionManager.EXPECT().UpdateSubscription(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			err := tWorker.handler(ctx, id)
			So(err, ShouldBeNil)
			client.EXPECT().AddSubscription(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("error"))
			err = tWorker.handler(ctx, id)
			So(err, ShouldNotBeNil)
			So(len(sub.Filters), ShouldEqual, 1)
		})
		_ = tWorker.Close()
	})
}

func TestTriggerWorker_IsActive(t *testing.T) {
	Convey("test trigger worker isActive", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		active := tWorker.IsActive()
		So(active, ShouldBeFalse)
		tWorker.SetPhase(metadata.TriggerWorkerPhaseRunning)
		active = tWorker.IsActive()
		So(active, ShouldBeFalse)
		tWorker.Polish()
		active = tWorker.IsActive()
		So(active, ShouldBeTrue)
	})
}

func TestTriggerWorker_RemoteStartStop(t *testing.T) {
	Convey("test trigger worker start", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subscriptionManager := subscription.NewMockManager(ctrl)
		client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
		addr := "test"
		tWorker := NewTriggerWorkerByAddr(addr, subscriptionManager).(*triggerWorker)
		tWorker.client = client
		client.EXPECT().Start(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.RemoteStart(ctx)
		So(err, ShouldBeNil)
		client.EXPECT().Start(ctx, gomock.Any()).Return(nil, fmt.Errorf("error"))
		err = tWorker.RemoteStart(ctx)
		So(err, ShouldNotBeNil)
		client.EXPECT().Stop(ctx, gomock.Any()).Return(nil, nil)
		err = tWorker.RemoteStop(ctx)
		So(err, ShouldBeNil)
		client.EXPECT().Stop(ctx, gomock.Any()).Return(nil, fmt.Errorf("error"))
		err = tWorker.RemoteStop(ctx)
		So(err, ShouldNotBeNil)
		_ = tWorker.Close()
	})
}
