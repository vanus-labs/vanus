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

package reader

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetOffsetByTimestamp(t *testing.T) {
	Convey("test get offset by timestamp", t, func() {
		events := make(chan info.EventRecord, 10)
		r := NewReader(Config{}, events).(*reader)
		eventLogID := vanus.NewTestID()
		r.elReader[eventLogID] = struct{}{}
		rand.Seed(time.Now().Unix())
		offset := rand.Uint64()
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockEventlog := api.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockEventbus.EXPECT().GetLog(Any(), Any()).AnyTimes().Return(mockEventlog, nil)
		mockEventlog.EXPECT().QueryOffsetByTime(Any(), Any()).AnyTimes().Return(int64(offset), nil)
		r.config.Client = mockClient
		offsets, err := r.GetOffsetByTimestamp(context.Background(), time.Now().Unix())
		So(err, ShouldBeNil)
		So(len(offsets), ShouldEqual, 1)
		So(offsets[0].EventLogID, ShouldEqual, eventLogID)
		So(offsets[0].Offset, ShouldEqual, offset)
	})
}

func TestGetOffset(t *testing.T) {
	Convey("test get offset", t, func() {
		events := make(chan info.EventRecord, 10)
		r := NewReader(Config{}, events).(*reader)
		eventLogID := vanus.NewTestID()
		r.elReader = map[vanus.ID]struct{}{}
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockEventlog := api.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockEventbus.EXPECT().GetLog(Any(), Any()).AnyTimes().Return(mockEventlog, nil)
		r.config.Client = mockClient
		Convey("test latest", func() {
			r.config.OffsetType = primitive.LatestOffset
			rand.Seed(time.Now().Unix())
			offset := rand.Uint32()
			Convey("negative number", func() {
				mockEventlog.EXPECT().LatestOffset(Any()).AnyTimes().Return(int64(offset)*-1, nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, 0)
			})
			Convey("non negative number", func() {
				mockEventlog.EXPECT().LatestOffset(Any()).AnyTimes().Return(int64(offset), nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, offset)
			})
		})
		Convey("test earliest", func() {
			r.config.OffsetType = primitive.EarliestOffset
			rand.Seed(time.Now().Unix())
			offset := rand.Uint32()
			Convey("negative number", func() {
				mockEventlog.EXPECT().EarliestOffset(Any()).AnyTimes().Return(int64(offset)*-1, nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, 0)
			})
			Convey("non negative number", func() {
				mockEventlog.EXPECT().EarliestOffset(Any()).AnyTimes().Return(int64(offset), nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, offset)
			})
		})
		Convey("test timestamp", func() {
			r.config.OffsetType = primitive.Timestamp
			r.config.OffsetTimestamp = time.Now().Unix()
			rand.Seed(time.Now().Unix())
			offset := rand.Uint32()
			Convey("negative number", func() {
				mockEventlog.EXPECT().QueryOffsetByTime(Any(), Any()).AnyTimes().Return(int64(offset)*-1, nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, 0)
			})
			Convey("non negative number", func() {
				mockEventlog.EXPECT().QueryOffsetByTime(Any(), Any()).AnyTimes().Return(int64(offset), nil)
				v, err := r.getOffset(context.Background(), eventLogID)
				So(err, ShouldBeNil)
				So(v, ShouldEqual, offset)
			})
		})
		Convey("test exist", func() {
			rand.Seed(time.Now().Unix())
			offset := rand.Uint64()
			r.config.Offset = map[vanus.ID]uint64{eventLogID: offset}
			v, err := r.getOffset(context.Background(), eventLogID)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, offset)
		})
	})
}

func TestReaderStart(t *testing.T) {
	mockCtrl := NewController(t)
	defer mockCtrl.Finish()
	mockClient := client.NewMockClient(mockCtrl)
	mockEventbus := api.NewMockEventbus(mockCtrl)
	mockEventlog := api.NewMockEventlog(mockCtrl)
	mockBusWriter := api.NewMockBusWriter(mockCtrl)
	mockBusReader := api.NewMockBusReader(mockCtrl)
	mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
	mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
	mockEventbus.EXPECT().Reader(Any()).AnyTimes().Return(mockBusReader)
	mockEventbus.EXPECT().GetLog(Any(), Any()).AnyTimes().Return(mockEventlog, nil)
	mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
	mockEventlog.EXPECT().ID().AnyTimes().Return(uint64(0))

	Convey("test start eventLogs", t, func() {
		offset := int64(100)
		index := uint64(offset)
		mockEventlog.EXPECT().LatestOffset(Any()).AnyTimes().Return(offset, nil)
		mockEventlog.EXPECT().EarliestOffset(Any()).AnyTimes().Return(offset, nil)
		mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().DoAndReturn(
			func(ctx context.Context, opts ...api.ReadOption) ([]*ce.Event, int64, uint64, error) {
				time.Sleep(time.Millisecond)
				e := ce.NewEvent()
				e.SetID(uuid.NewString())
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, index)
				e.SetExtension(eventlog.XVanusLogOffset, buf)
				index++
				return []*ce.Event{&e}, int64(0), uint64(0), nil
			})
		eventCh := make(chan info.EventRecord, 100)
		r := NewReader(Config{EventBusName: "test"}, eventCh).(*reader)
		r.config.Client = mockClient
		r.Start()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for e := range eventCh {
				if e.Offset >= uint64(offset+100) {
					return
				}
			}
		}()
		wg.Wait()
		r.Close()
	})
}
