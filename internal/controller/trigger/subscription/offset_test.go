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
	"github.com/vanus-labs/vanus/client/pkg/api"

	"github.com/vanus-labs/vanus/internal/controller/trigger/metadata"
	"github.com/vanus-labs/vanus/internal/controller/trigger/secret"
	"github.com/vanus-labs/vanus/internal/controller/trigger/storage"
	"github.com/vanus-labs/vanus/internal/controller/trigger/subscription/offset"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
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
			{EventlogID: logID, Offset: offsetV},
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

func TestGetOrSaveOffsetOffset(t *testing.T) {
	Convey("test get or save offset", t, func() {
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
			{EventlogID: logID, Offset: offsetV},
		}
		Convey("get or save offset subscription no exist", func() {
			offsets, err := m.GetOrSaveOffset(ctx, noExistID)
			So(err, ShouldNotBeNil)
			So(len(offsets), ShouldEqual, 0)
		})
		Convey("get or save offset from storage offset", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).Return(listOffsetInfo, nil)
			offsets, err := m.GetOrSaveOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, len(listOffsetInfo))
			So(offsets[0].EventlogID, ShouldEqual, logID)
			So(offsets[0].Offset, ShouldEqual, offsetV)
		})
		Convey("get or save offset from client", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).AnyTimes().Return(nil, nil)
			offsets, err := m.GetOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 0)
			offsetManager.EXPECT().Offset(gomock.Any(), gomock.Any(), gomock.Any(), true).AnyTimes().Return(nil)
			mockEventbus := api.NewMockEventbus(ctrl)
			mockEventlog := api.NewMockEventlog(ctrl)
			mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
			mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockEventlog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
			mockEventlog.EXPECT().LatestOffset(gomock.Any()).AnyTimes().Return(int64(offsetV), nil)
			Convey("test get offset from latest", func() {
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventlogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
			Convey("test get offset from earliest", func() {
				sub.Config.OffsetType = primitive.EarliestOffset
				m.UpdateSubscription(ctx, sub)
				mockEventlog.EXPECT().EarliestOffset(gomock.Any()).Return(int64(offsetV), nil)
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventlogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
			Convey("test get offset from timestamp", func() {
				sub.Config.OffsetType = primitive.Timestamp
				time := uint64(time.Now().Unix())
				sub.Config.OffsetTimestamp = &time
				m.UpdateSubscription(ctx, sub)
				mockEventlog.EXPECT().QueryOffsetByTime(gomock.Any(), int64(time)).Return(int64(offsetV), nil)
				offsets, err = m.GetOrSaveOffset(ctx, id)
				So(err, ShouldBeNil)
				So(len(offsets), ShouldEqual, 2*len(listOffsetInfo))
				So(offsets[0].EventlogID, ShouldEqual, logID)
				So(offsets[0].Offset, ShouldEqual, offsetV)
			})
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
			{EventlogID: logID, Offset: offsetV},
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
			So(offsets[0].EventlogID, ShouldEqual, logID)
			So(offsets[0].Offset, ShouldEqual, offsetV)
		})
		Convey("get offset from storage offset is empty", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).Return(nil, nil)
			offsets, err := m.GetOffset(ctx, id)
			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 0)
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
		mockEventlog := api.NewMockEventlog(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
		mockEventlog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
		time := uint64(time.Now().Unix())
		mockEventlog.EXPECT().QueryOffsetByTime(gomock.Any(), int64(time)).Return(int64(100), nil)
		_, err := m.ResetOffsetByTimestamp(ctx, id, time)
		So(err, ShouldBeNil)
	})
}

func TestGetDeadLetterOffset(t *testing.T) {
	Convey("test get dead letter offset", t, func() {
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
		noExistID := vanus.NewTestID()
		id := vanus.NewTestID()
		sub := &metadata.Subscription{
			ID:         id,
			EventbusID: vanus.NewTestID(),
			CreatedAt:  time.Now(),
		}
		m.AddSubscription(ctx, sub)
		logID := vanus.NewTestID()
		offsetV := uint64(10)
		listOffsetInfo := info.ListOffsetInfo{
			{EventlogID: logID, Offset: offsetV},
		}
		Convey("get dead letter offset subscription no exist", func() {
			offset, err := m.GetDeadLetterOffset(ctx, noExistID)
			So(err, ShouldNotBeNil)
			So(offset, ShouldEqual, 0)
		})
		Convey("get dead letter offset from storage offset", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).Return(listOffsetInfo, nil)
			Convey("dead letter eventlogID has init", func() {
				m.deadLetterEventlogMap[sub.EventbusID] = logID
				offset, err := m.GetDeadLetterOffset(ctx, id)
				So(err, ShouldBeNil)
				So(offset, ShouldEqual, offsetV)
			})
			Convey("dead letter eventlogID hasn't init", func() {
				mockEventbus := api.NewMockEventbus(ctrl)
				mockEventlog := api.NewMockEventlog(ctrl)
				mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
				mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
				mockEventlog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
				offset, err := m.GetDeadLetterOffset(ctx, id)
				So(err, ShouldBeNil)
				So(offset, ShouldEqual, offsetV)
			})
		})
		Convey("get dead letter offset from client", func() {
			offsetManager.EXPECT().GetOffset(ctx, id).AnyTimes().Return(nil, nil)
			offsetManager.EXPECT().Offset(gomock.Any(), gomock.Any(), gomock.Any(), true).AnyTimes().Return(nil)
			mockEventbus := api.NewMockEventbus(ctrl)
			mockEventlog := api.NewMockEventlog(ctrl)
			mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
			mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockEventlog.EXPECT().ID().AnyTimes().Return(logID.Uint64())

			Convey("test get offset from timestamp", func() {
				mockEventlog.EXPECT().QueryOffsetByTime(gomock.Any(),
					sub.CreatedAt.Unix()).Return(int64(offsetV), nil)
				offset, err := m.GetDeadLetterOffset(ctx, id)
				So(err, ShouldBeNil)
				So(offset, ShouldEqual, offsetV)
			})
		})
	})
}

func TestSaveDeadLetterOffset(t *testing.T) {
	Convey("test save dead letter offset", t, func() {
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
			ID:         id,
			EventbusID: vanus.NewTestID(),
			CreatedAt:  time.Now(),
		}
		m.AddSubscription(ctx, sub)
		logID := vanus.NewTestID()
		offsetV := uint64(10)
		offsetManager.EXPECT().Offset(gomock.Any(), gomock.Any(), gomock.Any(), true).AnyTimes().Return(nil)
		Convey("save dead letter offset subscription no exist", func() {
			err := m.SaveDeadLetterOffset(ctx, vanus.NewTestID(), offsetV)
			So(err, ShouldNotBeNil)
		})
		Convey("save dead letter offset eventlogID has init", func() {
			m.deadLetterEventlogMap[sub.EventbusID] = logID
			err := m.SaveDeadLetterOffset(ctx, id, offsetV)
			So(err, ShouldBeNil)
		})
		Convey("dead letter eventlogID hasn't init", func() {
			mockEventbus := api.NewMockEventbus(ctrl)
			mockEventlog := api.NewMockEventlog(ctrl)
			mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
			mockEventbus.EXPECT().ListLog(gomock.Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockEventlog.EXPECT().ID().AnyTimes().Return(logID.Uint64())
			err := m.SaveDeadLetterOffset(ctx, id, offsetV)
			So(err, ShouldBeNil)
		})
	})
}
