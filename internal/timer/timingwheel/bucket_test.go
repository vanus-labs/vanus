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

package timingwheel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"

	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTimingMsg_newTimingMsg(t *testing.T) {
	Convey("test timing message new", t, func() {
		ctx := context.Background()
		e := ce.NewEvent()
		e.SetID("1")

		Convey("test timing message new1", func() {
			t := time.Now().Add(time.Second).UTC().Format("2006-01-02T15:04:05")
			e.SetExtension(xceVanusDeliveryTime, t)
			tm := newTimingMsg(ctx, &e)
			So(time.Now().Add(100*time.Millisecond).After(tm.expiration), ShouldBeTrue)
		})

		Convey("test timing message new2", func() {
			tm := newTimingMsg(ctx, &e)
			So(time.Now().Add(100*time.Millisecond).After(tm.expiration), ShouldBeTrue)
		})
	})
}

func TestTimingMsg_isExpired(t *testing.T) {
	Convey("test timing message is expired", t, func() {
		ctx := context.Background()
		e := ce.NewEvent()
		e.SetExtension(xceVanusDeliveryTime, time.Now().Add(2*time.Second).UTC().Format("2006-01-02T15:04:05Z"))
		tm := newTimingMsg(ctx, &e)
		So(tm.isExpired(1*time.Second), ShouldEqual, false)
	})
}

func TestTimingMsg_consume(t *testing.T) {
	Convey("test timing message consume", t, func() {
		ctx := context.Background()
		e := event(2000)
		mockCtrl := gomock.NewController(t)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test bucket flush timing message", func() {
			tm := newTimingMsg(ctx, e)
			err := tm.consume(ctx, []string{})
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket fluent timing message return false1", func() {
			ee := ce.NewEvent()
			ee.SetExtension(xceVanusEventbus, time.Now())
			tm := newTimingMsg(ctx, &ee)
			err := tm.consume(ctx, []string{})
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket fluent timing message return false2", func() {
			stubs := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stubs.Reset()
			tm := newTimingMsg(ctx, e)
			err := tm.consume(ctx, []string{})
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket fluent timing message return false3", func() {
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, nil, errors.New("test"))
			defer stub2.Reset()
			tm := newTimingMsg(ctx, e)
			err := tm.consume(ctx, []string{})
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket fluent timing message return false4", func() {
			mockEventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(0), errors.New("test"))
			mockEventlogWriter.EXPECT().Close().Times(1).Return()
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			tm := newTimingMsg(ctx, e)
			err := tm.consume(ctx, []string{})
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket fluent timing message return false5", func() {
			mockEventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(0), nil)
			mockEventlogWriter.EXPECT().Close().Times(1).Return()
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			tm := newTimingMsg(ctx, e)
			err := tm.consume(ctx, []string{})
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_newBucket(t *testing.T) {
	Convey("test bucket new", t, func() {
		bucket := newBucket(cfg(), nil, nil, 1, "quick-start", 1, 1)
		So(bucket.config.CtrlEndpoints, ShouldResemble, []string{"127.0.0.1"})
		So(bucket.tick, ShouldEqual, 1)
		So(bucket.offset, ShouldEqual, 0)
		So(bucket.eventbus, ShouldEqual, "quick-start")
	})
}

func TestBuckets_createBucketsForTimingWheel(t *testing.T) {
	Convey("test buckets create buckets for timingwheel", t, func() {
		buckets := createBucketsForTimingWheel(cfg(), nil, nil, 1, 1)
		So(len(buckets), ShouldEqual, 10)
		So(buckets[0].config.CtrlEndpoints, ShouldResemble, []string{"127.0.0.1"})
		So(buckets[0].tick, ShouldEqual, 1)
		So(buckets[0].offset, ShouldEqual, 0)
		So(buckets[0].eventbus, ShouldEqual, fmt.Sprintf(timerBuiltInEventbus, 1, 0))
	})
}

func TestBucket_start(t *testing.T) {
	Convey("test bucket start", t, func() {
		ctx := context.Background()
		bucket := newBucket(cfg(), nil, nil, 1, "", 1, 1)
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test bucket strat failure", func() {
			stub1 := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stub1.Reset()
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket strat failure2", func() {
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, nil, errors.New("test"))
			defer stub2.Reset()
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket strat failure3", func() {
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, nil, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, errors.New("test"))
			defer stub3.Reset()
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket strat success", func() {
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, nil, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, nil)
			defer stub3.Reset()
			err := bucket.start(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_add(t *testing.T) {
	Convey("test bucket add", t, func() {
		ctx := context.Background()
		e := event(1000)
		bucket := newBucket(cfg(), nil, NewClient([]string{"127.0.0.1"}), 1, "__Timer_1_0", 1, 0)
		tm := newTimingMsg(ctx, e)
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		bucket.client.leaderClient = mockEventbusCtrlCli
		bucket.eventlogWriter = mockEventlogWriter
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test bucket add failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{Name: "__Timer_1_0"}).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			err := bucket.add(ctx, tm, true)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket add success", func() {
			mockEventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(1), nil)
			err := bucket.add(ctx, tm, false)
			So(err, ShouldBeNil)
		})

		Convey("test bucket add start failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{Name: "__Timer_1_0"}).AnyTimes().Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).AnyTimes().Return(nil, nil)
			stub1 := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stub1.Reset()
			err := bucket.add(ctx, tm, true)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestBucket_fetchEventFromOverflowWheelAdvance(t *testing.T) {
	Convey("test bucket fetch event from overflowwheel advance", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		bucket := newBucket(cfg(), nil, nil, time.Second, "", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.eventlogReader = mockEventlogReader
		bucket.eventlogWriter = mockEventlogWriter
		bucket.kvStore = mockStoreCli

		Convey("test bucket fetch event from overflowwheel advance1", func() {
			events := make([]*ce.Event, 1)
			events[0] = event(500)
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Eq(ctx), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventlogWriter.EXPECT().Append(ctx, gomock.Any()).AnyTimes().Return(int64(0), nil)
			mockEventlogWriter.EXPECT().Close().AnyTimes().Return()
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			add := func(context.Context, *timingMsg) bool {
				return false
			}
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.fetchEventFromOverflowWheelAdvance(ctx, add)
		})
	})
}

func TestBucket_createEventBus(t *testing.T) {
	Convey("test bucket create eventbus", t, func() {
		ctx := context.Background()
		bucket := newBucket(cfg(), nil, NewClient([]string{"127.0.0.1"}), 1, "__Timer_1_0", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.client.leaderClient = mockEventbusCtrlCli

		Convey("test bucket create eventbus success", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, nil)
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).AnyTimes().Return(nil, nil)
			err := bucket.createEventbus(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_putEvent(t *testing.T) {
	Convey("test bucket put event", t, func() {
		ctx := context.Background()
		e := event(1000)
		bucket := newBucket(cfg(), nil, NewClient([]string{"127.0.0.1"}), 1, "", 1, 0)
		tm := newTimingMsg(ctx, e)
		mockCtrl := gomock.NewController(t)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		bucket.eventlogWriter = mockEventlogWriter

		Convey("test bucket put event success", func() {
			mockEventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(1), errors.New("test"))
			err := bucket.putEvent(ctx, tm)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestBucket_getEvent(t *testing.T) {
	Convey("test bucket get event", t, func() {
		ctx := context.Background()
		bucket := newBucket(cfg(), nil, nil, 1, "", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		bucket.eventlogReader = mockEventlogReader

		Convey("test bucket get event with seek error", func() {
			events := make([]*ce.Event, 1)
			events[0] = event(10000)
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), errors.New("test"))
			mockEventlogReader.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, nil)
			result, err := bucket.getEvent(ctx, 1)
			So(result, ShouldBeNil)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestBucket_updateOffsetMeta(t *testing.T) {
	Convey("test bucket update offset metadata", t, func() {
		ctx := context.Background()
		bucket := newBucket(cfg(), nil, nil, 1, "__Timer_1_0", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.kvStore = mockStoreCli

		Convey("test bucket update offset metadata success", func() {
			key := "/vanus/internal/resource/timer/metadata/offset/__Timer_1_0"
			offsetMeta := &metadata.OffsetMeta{
				Layer:    1,
				Slot:     0,
				Offset:   0,
				Eventbus: "__Timer_1_0",
			}
			data, _ := json.Marshal(offsetMeta)
			mockStoreCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			bucket.updateOffsetMeta(ctx, bucket.offset)
			time.Sleep(100 * time.Millisecond)
		})

		Convey("test bucket update offset metadata failure", func() {
			key := "/vanus/internal/resource/timer/metadata/offset/__Timer_1_0"
			offsetMeta := &metadata.OffsetMeta{
				Layer:    1,
				Slot:     0,
				Offset:   0,
				Eventbus: "__Timer_1_0",
			}
			data, _ := json.Marshal(offsetMeta)
			mockStoreCli.EXPECT().Set(ctx, key, data).Times(1).Return(errors.New("test"))
			bucket.updateOffsetMeta(ctx, bucket.offset)
			time.Sleep(100 * time.Millisecond)
		})
	})
}

func event(i int64) *ce.Event {
	e := ce.NewEvent()
	t := time.Now().Add(time.Duration(i) * time.Millisecond).UTC().Format(time.RFC3339)
	e.SetExtension(xceVanusDeliveryTime, t)
	e.SetExtension(xceVanusEventbus, "quick-start")
	return &e
}
