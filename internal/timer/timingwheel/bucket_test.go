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
	"errors"
	"io"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/kv"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"

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
			e.SetExtension(xVanusDeliveryTime, t)
			tm := newTimingMsg(ctx, &e)
			So(time.Now().Add(100*time.Millisecond).After(tm.expiration), ShouldBeTrue)
		})

		Convey("test timing message new2", func() {
			tm := newTimingMsg(ctx, &e)
			So(time.Now().Add(100*time.Millisecond).After(tm.expiration), ShouldBeTrue)
		})
	})
}

func TestTimingMsg_hasExpired(t *testing.T) {
	Convey("test timing message is expired", t, func() {
		ctx := context.Background()
		e := ce.NewEvent()
		e.SetExtension(xVanusDeliveryTime, time.Now().Add(2*time.Second).UTC().Format(time.RFC3339))
		tm := newTimingMsg(ctx, &e)
		So(tm.hasExpired(), ShouldEqual, false)
	})
}

func TestBucket_newBucket(t *testing.T) {
	Convey("test bucket new", t, func() {
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "quick-start", 1, 1)
		So(bucket.tick, ShouldEqual, 1)
		So(bucket.offset, ShouldEqual, 0)
		So(bucket.eventbus, ShouldEqual, "quick-start")
	})
}

func TestBucket_start(t *testing.T) {
	Convey("test bucket start", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		bucket.timingwheel.leader = true
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.client = mockEventbusCtrlCli
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test bucket start with create eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket start with connect eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			stubs := StubFunc(&openBusWriter, nil, errors.New("test"))
			defer stubs.Reset()
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket start success", func() {
			bucket.timingwheel.leader = false
			stub1 := StubFunc(&openBusWriter, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, nil)
			defer stub3.Reset()
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			err := bucket.start(ctx)
			bucket.wg.Wait()
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_run(t *testing.T) {
	Convey("test bucket run", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		tw.distributionStation.timingwheel = tw
		bucket := newBucket(tw, nil, time.Second, "", 1, 0)
		bucket.timingwheel = tw
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.eventlogReader = mockEventlogReader
		bucket.eventbusWriter = mockEventbusWriter
		bucket.kvStore = mockStoreCli
		tw.distributionStation.eventbusWriter = mockEventbusWriter
		events := make([]*ce.Event, 1)
		events[0] = event(0)
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.eventlogReader = mockEventlogReader
				bucket.timingwheel = tw
			}
		}

		Convey("test bucket run with get event failed", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, errors.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("test bucket run with push failed", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", errors.New("test"))
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("test bucket run with high layer push failed", func() {
			events[0] = event(1000)
			bucket.layer = 2
			bucket.element = tw.twList.Front().Next()
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", errors.New("test"))
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("test bucket run success", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", nil)
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})
	})
}

func TestBucket_createEventBus(t *testing.T) {
	Convey("test bucket create eventbus", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		bucket := newBucket(tw, nil, time.Second, "", 1, 0)
		bucket.timingwheel = tw
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.client = mockEventbusCtrlCli

		Convey("test bucket create with eventbus exist", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			err := bucket.createEventbus(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_connectEventbus(t *testing.T) {
	Convey("test bucket connect eventbus", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test bucket connect eventbus with open eventbus writer failed", func() {
			stub1 := StubFunc(&openBusWriter, nil, errors.New("test"))
			defer stub1.Reset()
			err := bucket.connectEventbus(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket connect eventbus with lookup readable logs failed", func() {
			stub1 := StubFunc(&openBusWriter, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stub2.Reset()
			err := bucket.connectEventbus(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket connect eventbus with open eventlog reader failed", func() {
			stub1 := StubFunc(&openBusWriter, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, errors.New("test"))
			defer stub3.Reset()
			err := bucket.connectEventbus(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket connect eventbus success", func() {
			stub1 := StubFunc(&openBusWriter, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, nil)
			defer stub3.Reset()
			err := bucket.connectEventbus(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_putEvent(t *testing.T) {
	Convey("test bucket put event", t, func() {
		ctx := context.Background()
		e := event(1000)
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		bucket.timingwheel = tw

		Convey("test bucket put event success", func() {
			err := bucket.putEvent(ctx, newTimingMsg(ctx, e))
			So(err, ShouldBeNil)
		})

		Convey("test bucket put event panic", func() {
			bucket.eventbusWriter = nil
			tw.SetLeader(true)
			err := bucket.putEvent(ctx, newTimingMsg(ctx, e))
			So(err, ShouldNotBeNil)
		})
	})
}

func TestBucket_getEvent(t *testing.T) {
	Convey("test bucket get event", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		bucket.timingwheel = tw
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		bucket.eventlogReader = mockEventlogReader

		Convey("test bucket get event success", func() {
			result, err := bucket.getEvent(ctx, 1)
			So(len(result), ShouldEqual, 0)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket get event panic", func() {
			bucket.eventlogReader = nil
			tw.SetLeader(true)
			result, err := bucket.getEvent(ctx, 1)
			So(len(result), ShouldEqual, 0)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket get event with seek error", func() {
			tw.SetLeader(true)
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
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		bucket.timingwheel = tw
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.kvStore = mockStoreCli

		Convey("test bucket update offset metadata with follower", func() {
			tw.SetLeader(false)
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})

		Convey("test bucket update offset metadata success", func() {
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})

		Convey("test bucket update offset metadata failure", func() {
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(errors.New("test"))
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})
	})
}

func TestBucket_wait(t *testing.T) {
	Convey("test bucket wait", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)

		Convey("test bucket wait success", func() {
			bucket.wait(ctx)
		})
	})
}

func TestBucket_getOffset(t *testing.T) {
	Convey("test bucket get offset", t, func() {
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)

		Convey("test bucket get offset success", func() {
			offset := bucket.getOffset()
			So(offset, ShouldEqual, 0)
		})
	})
}

func event(i int64) *ce.Event {
	e := ce.NewEvent()
	t := time.Now().Add(time.Duration(i) * time.Millisecond).UTC().Format(time.RFC3339)
	e.SetExtension(xVanusDeliveryTime, t)
	e.SetExtension(xVanusEventbus, "quick-start")
	return &e
}
