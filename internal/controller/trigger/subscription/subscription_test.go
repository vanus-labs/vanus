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

package subscription

import (
	"context"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestInit(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	m := NewSubscriptionManager(storage)

	Convey("init ", t, func() {
		storage.MockSubscriptionStorage.EXPECT().ListSubscription(ctx).Return([]*primitive.SubscriptionData{
			{ID: 1},
		}, nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
	})
	Convey("start stop", t, func() {
		m.Start()
		time.Sleep(time.Millisecond * 10)
		m.Stop()
	})
}

func TestSubscriptionData(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	m := NewSubscriptionManager(storage)
	Convey("list subscription size 0", t, func() {
		subscriptionMap := m.ListSubscription(ctx)
		So(len(subscriptionMap), ShouldEqual, 0)
	})
	Convey("get subscription not exist", t, func() {
		subscription := m.GetSubscriptionData(ctx, 1)
		So(subscription, ShouldBeNil)
	})
	Convey("add subscription", t, func() {
		storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
		err := m.AddSubscription(ctx, makeSubscription())
		So(err, ShouldBeNil)
	})
	var ID vanus.ID
	var subscriptionData *primitive.SubscriptionData
	Convey("list subscription", t, func() {
		subscriptionMap := m.ListSubscription(ctx)
		So(len(subscriptionMap), ShouldEqual, 1)
		for id, data := range subscriptionMap {
			ID = id
			subscriptionData = data
		}
	})
	Convey("get subscription data", t, func() {
		subscription := m.GetSubscriptionData(ctx, ID)
		So(subscription, ShouldNotBeNil)
	})
	Convey("update subscription", t, func() {
		storage.MockSubscriptionStorage.EXPECT().UpdateSubscription(ctx, gomock.Any()).Return(nil)
		subscriptionData.Sink = "newSink"
		err := m.UpdateSubscription(ctx, subscriptionData)
		So(err, ShouldBeNil)
		subscription := m.GetSubscriptionData(ctx, ID)
		So(subscription.Sink, ShouldEqual, subscriptionData.Sink)
	})
	Convey("heartbeat", t, func() {
		storage.MockSubscriptionStorage.EXPECT().UpdateSubscription(ctx, gomock.Any()).Return(nil)
		err := m.Heartbeat(ctx, ID, "addr", time.Now())
		So(err, ShouldBeNil)
	})
}
func TestOffset(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	m := NewSubscriptionManager(storage).(*manager)
	offsetManager := offset.NewMockManager(ctrl)
	m.offsetManager = offsetManager
	storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
	m.AddSubscription(ctx, makeSubscription())
	var ID vanus.ID
	subscriptionMap := m.ListSubscription(ctx)
	for id := range subscriptionMap {
		ID = id
	}
	listOffsetInfo := info.ListOffsetInfo{
		{EventLogID: 1, Offset: 10},
	}
	Convey("set offset ", t, func() {
		err := m.Offset(ctx, 1, listOffsetInfo)
		So(err, ShouldBeNil)
		offsetManager.EXPECT().Offset(ctx, ID, listOffsetInfo).Return(nil)
		err = m.Offset(ctx, ID, listOffsetInfo)
		So(err, ShouldBeNil)
	})
	Convey("get offset", t, func() {
		offsets, err := m.GetOffset(ctx, 1)
		So(err, ShouldBeNil)
		So(len(offsets), ShouldEqual, 0)
		offsetManager.EXPECT().GetOffset(ctx, ID).Return(listOffsetInfo, nil)
		offsets, err = m.GetOffset(ctx, ID)
		So(err, ShouldBeNil)
		So(len(offsets), ShouldEqual, 1)
		So(offsets[0].EventLogID, ShouldEqual, 1)
		So(offsets[0].Offset, ShouldEqual, 10)
	})
}

func TestSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	m := NewSubscriptionManager(storage).(*manager)
	offsetManager := offset.NewMockManager(ctrl)
	m.offsetManager = offsetManager
	storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
	m.AddSubscription(ctx, makeSubscription())
	var ID vanus.ID
	subscriptionMap := m.ListSubscription(ctx)
	for id := range subscriptionMap {
		ID = id
	}
	listOffsetInfo := info.ListOffsetInfo{
		{EventLogID: 1, Offset: 10},
	}
	offsetManager.EXPECT().GetOffset(ctx, ID).Return(listOffsetInfo, nil)
	Convey("get subscription", t, func() {
		subscription, err := m.GetSubscription(ctx, ID)
		So(err, ShouldBeNil)
		So(subscription, ShouldNotBeNil)
		So(subscription.Offsets[0].Offset, ShouldEqual, listOffsetInfo[0].Offset)
		So(subscription.Offsets[0].EventLogID, ShouldEqual, listOffsetInfo[0].EventLogID)
	})
	Convey("delete subscription data", t, func() {
		offsetManager.EXPECT().RemoveRegisterSubscription(ctx, gomock.Any()).Return(nil)
		storage.MockSubscriptionStorage.EXPECT().DeleteSubscription(ctx, gomock.Any()).Return(nil)
		err := m.DeleteSubscription(ctx, ID)
		So(err, ShouldBeNil)
		subscription, err := m.GetSubscription(ctx, ID)
		So(err, ShouldNotBeNil)
		So(subscription, ShouldBeNil)
	})
}

func makeSubscription() *primitive.SubscriptionData {
	return &primitive.SubscriptionData{}
}
