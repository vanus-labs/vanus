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
	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/controller/trigger/secret"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSaveOffset(t *testing.T) {
	Convey("test save offset", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)

		m := NewSubscriptionManager(storage, secret, nil).(*manager)
		offsetManager := offset.NewMockManager(ctrl)
		m.offsetManager = offsetManager
		storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
		noExistID := vanus.NewTestID()
		id := vanus.NewTestID()
		sub := &metadata.Subscription{
			ID: id,
		}
		m.AddSubscription(ctx, sub)
		logID := vanus.NewTestID()
		offsetV := uint64(10)
		listOffsetInfo := info.ListOffsetInfo{
			{EventLogID: logID, Offset: offsetV},
		}
		Convey("save offset subscription no exist", func() {
			err := m.SaveOffset(ctx, noExistID, listOffsetInfo, false)
			So(err, ShouldBeNil)
		})
		Convey("save offset valid", func() {
			offsetManager.EXPECT().Offset(ctx, id, listOffsetInfo, false).Return(nil)
			err := m.SaveOffset(ctx, id, listOffsetInfo, false)
			So(err, ShouldBeNil)
		})
	})
}

func TestGetOffset(t *testing.T) {
	Convey("test get offset", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)
		mockClient := client.NewMockClient(ctrl)

		m := NewSubscriptionManager(storage, secret, mockClient).(*manager)
		offsetManager := offset.NewMockManager(ctrl)
		m.offsetManager = offsetManager
		storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
		storage.MockSubscriptionStorage.EXPECT().UpdateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
		noExistID := vanus.NewTestID()
		id := vanus.NewTestID()
		sub := &metadata.Subscription{
			ID: id,
		}
		m.AddSubscription(ctx, sub)
		logID := vanus.NewTestID()
		offsetV := uint64(10)
		listOffsetInfo := info.ListOffsetInfo{
			{EventLogID: logID, Offset: offsetV},
		}
		Convey("get offset subscription no exist", func() {
			offsets, err := m.GetOffset(ctx, noExistID)
			So(err, ShouldNotBeNil)
			So(len(offsets), ShouldEqual, 0)
		})
		Convey("get offset from storage offset", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).Return(listOffsetInfo, nil)
			offsets, err := m.GetOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, len(listOffsetInfo))
			So(offsets[0].EventLogID, ShouldEqual, logID)
			So(offsets[0].Offset, ShouldEqual, offsetV)
		})
		Convey("get offset from client", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).AnyTimes().Return(nil, nil)
			offsets, err := m.GetOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 0)
			offsetManager.EXPECT().Offset(gomock.Any(), gomock.Any(), gomock.Any(), true).AnyTimes().Return(nil)
			mockEventbus := api.NewMockEventbus(ctrl)
			mockEventLog := api.NewMockEventlog(ctrl)
			mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
			mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventLog}, nil)
			mockEventLog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
			mockEventLog.EXPECT().LatestOffset(gomock.Any()).AnyTimes().Return(int64(offsetV), nil)
			Convey("test get offset from latest", func() {
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventLogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
			Convey("test get offset from earliest", func() {
				sub.Config.OffsetType = primitive.EarliestOffset
				m.UpdateSubscription(ctx, sub)
				mockEventLog.EXPECT().EarliestOffset(gomock.Any()).Return(int64(offsetV), nil)
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventLogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
			Convey("test get offset from timestamp", func() {
				sub.Config.OffsetType = primitive.Timestamp
				time := uint64(time.Now().Unix())
				sub.Config.OffsetTimestamp = &time
				m.UpdateSubscription(ctx, sub)
				mockEventLog.EXPECT().QueryOffsetByTime(gomock.Any(), int64(time)).Return(int64(offsetV), nil)
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventLogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
		})
	})
}

func TestResetOffsetByTimestamp(t *testing.T) {
	Convey("test reset offset by timestamp", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := storage.NewMockStorage(ctrl)
		secret := secret.NewMockStorage(ctrl)
		mockClient := client.NewMockClient(ctrl)

		m := NewSubscriptionManager(storage, secret, mockClient).(*manager)
		offsetManager := offset.NewMockManager(ctrl)
		m.offsetManager = offsetManager
		storage.MockSubscriptionStorage.EXPECT().CreateSubscription(ctx, gomock.Any()).AnyTimes().Return(nil)
		id := vanus.NewTestID()
		sub := &metadata.Subscription{
			ID: id,
		}
		m.AddSubscription(ctx, sub)
		logID := vanus.NewTestID()

		offsetManager.EXPECT().Offset(ctx, id, gomock.Any(), true).Return(nil)
		mockEventbus := api.NewMockEventbus(ctrl)
		mockEventLog := api.NewMockEventlog(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventLog}, nil)
		mockEventLog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
		time := uint64(time.Now().Unix())
		mockEventLog.EXPECT().QueryOffsetByTime(gomock.Any(), int64(time)).Return(int64(100), nil)
		_, err := m.ResetOffsetByTimestamp(ctx, id, time)
		So(err, ShouldBeNil)
	})
}
