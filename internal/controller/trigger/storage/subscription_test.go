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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewSubscriptionStorage(kvClient).(*subscriptionStorage)
	subID := vanus.ID(1)
	Convey("create subscription", t, func() {
		kvClient.EXPECT().Create(ctx, s.getKey(subID), gomock.Any()).Return(nil)
		err := s.CreateSubscription(ctx, &metadata.Subscription{
			ID: subID,
		})
		So(err, ShouldBeNil)
	})
}

func TestUpdateSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewSubscriptionStorage(kvClient).(*subscriptionStorage)
	subID := vanus.ID(1)
	Convey("update subscription", t, func() {
		kvClient.EXPECT().Update(ctx, s.getKey(subID), gomock.Any()).Return(nil)
		err := s.UpdateSubscription(ctx, &metadata.Subscription{
			ID: subID,
		})
		So(err, ShouldBeNil)
	})
}

func TestGetSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewSubscriptionStorage(kvClient).(*subscriptionStorage)
	subID := vanus.ID(1)
	Convey("get subscription", t, func() {
		expect := &metadata.Subscription{
			ID:       subID,
			EventBus: "bus",
		}
		v, _ := json.Marshal(expect)
		kvClient.EXPECT().Get(ctx, s.getKey(subID)).Return(v, nil)
		data, err := s.GetSubscription(ctx, subID)
		So(err, ShouldBeNil)
		So(data.EventBus, ShouldEqual, expect.EventBus)
	})
}

func TestDeleteSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewSubscriptionStorage(kvClient).(*subscriptionStorage)
	subID := vanus.ID(1)
	Convey("delete subscription", t, func() {
		kvClient.EXPECT().Delete(ctx, s.getKey(subID)).Return(nil)
		err := s.DeleteSubscription(ctx, subID)
		So(err, ShouldBeNil)
	})
}

func TestListSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewSubscriptionStorage(kvClient).(*subscriptionStorage)
	subID := vanus.ID(1)
	Convey("list subscription", t, func() {
		expect := &metadata.Subscription{
			ID:       subID,
			EventBus: "bus",
		}
		v, _ := json.Marshal(expect)
		kvClient.EXPECT().List(ctx, KeyPrefixSubscription.String()).Return([]kv.Pair{
			{Key: fmt.Sprintf("%d", subID), Value: v},
		}, nil)
		list, err := s.ListSubscription(ctx)
		So(err, ShouldBeNil)
		So(len(list), ShouldEqual, 1)
		So(list[0].EventBus, ShouldEqual, expect.EventBus)
	})
}
