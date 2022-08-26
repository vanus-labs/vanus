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

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/secret"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriptionInit(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	secret := secret.NewMockStorage(ctrl)
	m := NewSubscriptionManager(storage, secret)

	Convey("init ", t, func() {
		subID := vanus.NewID()
		credentialType := primitive.Cloud
		storage.MockSubscriptionStorage.EXPECT().ListSubscription(ctx).Return([]*metadata.Subscription{
			{ID: subID, SinkCredentialType: &credentialType},
		}, nil)
		secret.EXPECT().Read(gomock.Any(), gomock.Eq(subID), gomock.Any()).
			Return(primitive.NewCloudSinkCredential("ak", "sk"), nil)
		err := m.Init(ctx)
		So(err, ShouldBeNil)
	})
	Convey("start stop", t, func() {
		m.Start()
		time.Sleep(time.Millisecond * 10)
		m.Stop()
	})
}

func TestGetListSubscription(t *testing.T) {
	Convey("test get list subscription", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)
		m := NewSubscriptionManager(storage, secret)
		id := vanus.NewID()
		Convey("list subscription size 0", func() {
			subscriptions := m.ListSubscription(ctx)
			So(len(subscriptions), ShouldEqual, 0)
		})
		Convey("get subscription not exist", func() {
			subscription := m.GetSubscription(ctx, id)
			So(subscription, ShouldBeNil)
		})
		Convey("heartbeat subscription no exist", func() {
			err := m.Heartbeat(ctx, id, "addr", time.Now())
			So(err, ShouldNotBeNil)
		})
		storage.MockSubscriptionStorage.EXPECT().ListSubscription(ctx).Return([]*metadata.Subscription{
			{ID: id},
		}, nil)
		_ = m.Init(ctx)
		Convey("list subscription", func() {
			subscriptions := m.ListSubscription(ctx)
			So(len(subscriptions), ShouldEqual, 1)
			So(subscriptions[0].ID, ShouldEqual, id)
		})
		Convey("get subscription", func() {
			subscription := m.GetSubscription(ctx, id)
			So(subscription, ShouldNotBeNil)
		})
		Convey("heartbeat", func() {
			err := m.Heartbeat(ctx, id, "addr", time.Now())
			So(err, ShouldBeNil)
		})
	})
}

func TestSubscription(t *testing.T) {
	Convey("test add update delete subscription", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)
		m := NewSubscriptionManager(storage, secret)
		subID := vanus.NewID()
		Convey("test add subscription", func() {
			storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
			secret.EXPECT().Write(gomock.Any(), gomock.Eq(subID), gomock.Any()).Return(nil)
			credentialType := primitive.Cloud
			err := m.AddSubscription(ctx, &metadata.Subscription{
				ID:                 subID,
				SinkCredentialType: &credentialType,
				SinkCredential:     primitive.NewCloudSinkCredential("test_ak", "test_sk"),
			})
			So(err, ShouldBeNil)
			Convey("test update subscription", func() {
				storage.MockSubscriptionStorage.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
				Convey("update sink", func() {
					updateSub := &metadata.Subscription{
						ID:                 subID,
						SinkCredentialType: &credentialType,
						SinkCredential:     primitive.NewCloudSinkCredential("test_ak", "test_sk"),
						Sink:               "test",
					}
					err = m.UpdateSubscription(ctx, updateSub)
					So(err, ShouldBeNil)
					So(m.GetSubscription(ctx, subID).Sink, ShouldEqual, updateSub.Sink)
				})
				Convey("test update subscription credential", func() {
					Convey("modify credential", func() {
						updateSub := &metadata.Subscription{
							ID:                 subID,
							Sink:               "test",
							SinkCredentialType: &credentialType,
							SinkCredential:     primitive.NewCloudSinkCredential("test_new_ak", "test_new_sk"),
						}
						secret.EXPECT().Write(gomock.Any(), gomock.Eq(subID), gomock.Any()).Return(nil)
						err = m.UpdateSubscription(ctx, updateSub)
						So(err, ShouldBeNil)
					})
					Convey("delete credential", func() {
						updateSub := &metadata.Subscription{
							ID:                 subID,
							Sink:               "test",
							SinkCredentialType: nil,
							SinkCredential:     nil,
						}
						secret.EXPECT().Delete(gomock.Any(), gomock.Eq(subID)).Return(nil)
						err = m.UpdateSubscription(ctx, updateSub)
						So(err, ShouldBeNil)
					})
				})
			})
			Convey("test delete subscription", func() {
				mm := m.(*manager)
				offsetManager := offset.NewMockManager(ctrl)
				mm.offsetManager = offsetManager
				storage.MockSubscriptionStorage.EXPECT().DeleteSubscription(ctx, gomock.Eq(subID)).Return(nil)
				offsetManager.EXPECT().RemoveRegisterSubscription(ctx, gomock.Eq(subID)).Return(nil)
				secret.EXPECT().Delete(gomock.Any(), gomock.Eq(subID)).Return(nil)
				err = m.DeleteSubscription(ctx, subID)
				So(err, ShouldBeNil)
				So(m.GetSubscription(ctx, subID), ShouldBeNil)
			})
		})
	})
}

func TestOffset(t *testing.T) {
	Convey("test offset", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)
		m := NewSubscriptionManager(storage, secret).(*manager)
		offsetManager := offset.NewMockManager(ctrl)
		m.offsetManager = offsetManager
		storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
		m.AddSubscription(ctx, &metadata.Subscription{})
		subscriptions := m.ListSubscription(ctx)
		id := subscriptions[0].ID
		listOffsetInfo := info.ListOffsetInfo{
			{EventLogID: 1, Offset: 10},
		}
		Convey("set offset subscription no exist", func() {
			err := m.SaveOffset(ctx, 1, listOffsetInfo, false)
			So(err, ShouldBeNil)
		})
		Convey("set offset ", func() {
			offsetManager.EXPECT().Offset(ctx, id, listOffsetInfo, false).Return(nil)
			err := m.SaveOffset(ctx, id, listOffsetInfo, false)
			So(err, ShouldBeNil)
		})
		Convey("get offset subscription no exist", func() {
			offsets, err := m.GetOffset(ctx, 1)
			So(err, ShouldNotBeNil)
			So(len(offsets), ShouldEqual, 0)
		})
		Convey("get offset", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).Return(listOffsetInfo, nil)
			offsets, err := m.GetOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].EventLogID, ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, 10)
		})
	})
}
