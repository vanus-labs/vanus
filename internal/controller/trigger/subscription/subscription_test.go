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
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/internal/controller/trigger/metadata"
	"github.com/vanus-labs/vanus/internal/controller/trigger/secret"
	"github.com/vanus-labs/vanus/internal/controller/trigger/storage"
	"github.com/vanus-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/cluster"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func TestSubscriptionInit(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storage := storage.NewMockStorage(ctrl)
	secret := secret.NewMockStorage(ctrl)
	ebCli := client.NewMockClient(ctrl)
	cl := cluster.NewMockCluster(ctrl)
	m := NewSubscriptionManager(storage, secret, ebCli, cl)

	Convey("init ", t, func() {
		subID := vanus.NewTestID()
		credentialType := primitive.AWS
		storage.MockSubscriptionStorage.EXPECT().ListSubscription(ctx).Return([]*metadata.Subscription{
			{ID: subID, SinkCredentialType: &credentialType},
		}, nil)
		secret.EXPECT().Read(gomock.Any(), gomock.Eq(subID), gomock.Any()).
			Return(primitive.NewAkSkSinkCredential("ak", "sk"), nil)
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
		ebCli := client.NewMockClient(ctrl)
		cl := cluster.NewMockCluster(ctrl)
		m := NewSubscriptionManager(storage, secret, ebCli, cl)
		id := vanus.NewTestID()
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
		cl := cluster.NewMockCluster(ctrl)
		m := NewSubscriptionManager(storage, secret, nil, cl)
		subID := vanus.NewTestID()
		eventbusID := vanus.NewTestID()
		Convey("test add subscription", func() {
			storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).Return(nil)
			secret.EXPECT().Write(gomock.Any(), gomock.Eq(subID), gomock.Any()).Return(nil)
			credentialType := primitive.AWS
			ebService := cluster.NewMockEventbusService(ctrl)
			cl.EXPECT().EventbusService().AnyTimes().Return(ebService)
			ebService.EXPECT().GetSystemEventbusByName(gomock.Any(), gomock.Any()).AnyTimes().Return(&metapb.Eventbus{
				Id:   vanus.NewTestID().Uint64(),
				Logs: []*metapb.Eventlog{{EventlogId: vanus.NewTestID().Uint64()}},
			}, nil)
			err := m.AddSubscription(ctx, &metadata.Subscription{
				ID:                 subID,
				SinkCredentialType: &credentialType,
				SinkCredential:     primitive.NewAkSkSinkCredential("test_ak", "test_sk"),
				EventbusID:         eventbusID,
			})
			So(err, ShouldBeNil)
			Convey("test update subscription", func() {
				storage.MockSubscriptionStorage.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
				Convey("update sink", func() {
					updateSub := &metadata.Subscription{
						ID:                 subID,
						EventbusID:         eventbusID,
						SinkCredentialType: &credentialType,
						SinkCredential:     primitive.NewAkSkSinkCredential("test_ak", "test_sk"),
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
							SinkCredential:     primitive.NewAkSkSinkCredential("test_new_ak", "test_new_sk"),
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
