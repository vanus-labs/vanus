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
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	es "github.com/linkall-labs/vanus/client/pkg/errors"
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"

	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTimingWheel_NewTimingWheel(t *testing.T) {
	Convey("test timingwheel new", t, func() {
		Convey("test timingwheel new success", func() {
			stub1 := StubFunc(&newEtcdClientV3, nil, nil)
			defer stub1.Reset()
			So(NewTimingWheel(cfg()), ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_Start(t *testing.T) {
	Convey("test timingwheel start", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = mockEventlogReader
				bucket.client.leaderClient = mockEventbusCtrlCli
			}
			next := e.Next()
			e = next
		}
		tw.receivingStation = newBucket(tw.config, tw.kvStore, tw.client, 0, timerBuiltInEventbusReceivingStation, 0, 0)
		tw.distributionStation = newBucket(tw.config, tw.kvStore, tw.client, 0, timerBuiltInEventbusDistributionStation, 0, 0)

		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test timingwheel start bucket create eventbus failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			err := tw.Start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start bucket start failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, nil)
			stub1 := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stub1.Reset()
			err := tw.Start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start bucket start success", func() {
			tw.SetLeader(false)
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, gomock.Any()).AnyTimes().Return(nil, nil)
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, nil, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, mockEventlogReader, nil)
			defer stub3.Reset()
			err := tw.Start(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestTimingWheel_StopNotify(t *testing.T) {
	Convey("test timingwheel stop notify", t, func() {
		tw := newtimingwheel(cfg())
		retC := tw.StopNotify()
		So(retC, ShouldNotBeNil)
	})
}

func TestTimingWheel_Stop(t *testing.T) {
	Convey("test timingwheel stop", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.Stop(ctx)
	})
}

func TestTimingWheel_SetLeader(t *testing.T) {
	Convey("test timingwheel setleader", t, func() {
		tw := timingWheel{
			twList: list.New(),
			leader: false,
		}
		twe := &timingWheelElement{
			leader: false,
		}
		add(tw.twList, twe)
		add(tw.twList, twe)
		tw.SetLeader(true)
		So(tw.leader, ShouldBeTrue)
		So(tw.twList.Front().Value.(*timingWheelElement).leader, ShouldBeTrue)
		So(tw.twList.Back().Value.(*timingWheelElement).leader, ShouldBeTrue)
	})
}

func TestTimingWheel_IsLeader(t *testing.T) {
	Convey("test timingwheel isleader", t, func() {
		tw := timingWheel{
			leader: true,
		}
		So(tw.IsLeader(), ShouldBeTrue)
	})
}

func TestTimingWheel_IsDeployed(t *testing.T) {
	Convey("test timingwheel isdeployed", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		tw.client.leaderClient = mockEventbusCtrlCli

		Convey("test timingwheel is deployed return true", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, gomock.Any()).Times(2).Return(nil, nil)
			ret := tw.IsDeployed(ctx)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheel_RecoverForFailover(t *testing.T) {
	Convey("test timingwheel recover for failover", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.kvStore = mockStoreCli

		Convey("test timingwheel recover for failover first call failure", func() {
			path := "/vanus/internal/resource/timer/metadata/pointer"
			mockStoreCli.EXPECT().List(ctx, path).Times(1).Return(nil, errors.New("test"))
			err := tw.RecoverForFailover(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel recover for failover second call failure", func() {
			pointerPath := "/vanus/internal/resource/timer/metadata/pointer"
			pointerMD := metadata.PointerMeta{
				Layer:   1,
				Pointer: 1,
			}
			data, _ := json.Marshal(pointerMD)
			pointerKvPairs := make([]kv.Pair, 1)
			pointerKvPairs[0] = kv.Pair{
				Key:   "key",
				Value: data,
			}
			first := mockStoreCli.EXPECT().List(ctx, pointerPath).Times(1).Return(pointerKvPairs, nil)
			offsetPath := "/vanus/internal/resource/timer/metadata/offset"
			second := mockStoreCli.EXPECT().List(ctx, offsetPath).Times(1).Return(nil, errors.New("test"))
			gomock.InOrder(
				first,
				second,
			)
			err := tw.RecoverForFailover(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel recover for failover success", func() {
			pointerPath := "/vanus/internal/resource/timer/metadata/pointer"
			pointerMD := metadata.PointerMeta{
				Layer:   1,
				Pointer: 1,
			}
			data, _ := json.Marshal(pointerMD)
			pointerKvPairs := make([]kv.Pair, 1)
			pointerKvPairs[0] = kv.Pair{
				Key:   "key",
				Value: data,
			}
			first := mockStoreCli.EXPECT().List(ctx, pointerPath).Times(1).Return(pointerKvPairs, nil)
			offsetPath := "/vanus/internal/resource/timer/metadata/offset"
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    1,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_1_1",
			})
			offsetKvPairs := make([]kv.Pair, 3)
			offsetKvPairs[0] = kv.Pair{
				Key:   "0",
				Value: data,
			}
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    1,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_RS",
			})
			offsetKvPairs[1] = kv.Pair{
				Key:   "1",
				Value: data,
			}
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    1,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_DS",
			})
			offsetKvPairs[2] = kv.Pair{
				Key:   "2",
				Value: data,
			}
			second := mockStoreCli.EXPECT().List(ctx, offsetPath).Times(1).Return(offsetKvPairs, nil)
			gomock.InOrder(
				first,
				second,
			)
			err := tw.RecoverForFailover(ctx)
			So(err, ShouldBeNil)
		})

		Convey("test timingwheel recover for failover failure with highest layer", func() {
			pointerPath := "/vanus/internal/resource/timer/metadata/pointer"
			pointerMD := metadata.PointerMeta{
				Layer:   1,
				Pointer: 1,
			}
			data, _ := json.Marshal(pointerMD)
			pointerKvPairs := make([]kv.Pair, 1)
			pointerKvPairs[0] = kv.Pair{
				Key:   "key",
				Value: data,
			}
			first := mockStoreCli.EXPECT().List(ctx, pointerPath).Times(1).Return(pointerKvPairs, nil)
			offsetPath := "/vanus/internal/resource/timer/metadata/offset"
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    5,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_5_1",
			})
			offsetKvPairs := make([]kv.Pair, 3)
			offsetKvPairs[0] = kv.Pair{
				Key:   "0",
				Value: data,
			}
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    1,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_RS",
			})
			offsetKvPairs[1] = kv.Pair{
				Key:   "1",
				Value: data,
			}
			data, _ = json.Marshal(metadata.OffsetMeta{
				Layer:    1,
				Slot:     1,
				Offset:   1,
				Eventbus: "__Timer_DS",
			})
			offsetKvPairs[2] = kv.Pair{
				Key:   "2",
				Value: data,
			}
			second := mockStoreCli.EXPECT().List(ctx, offsetPath).Times(1).Return(offsetKvPairs, nil)
			gomock.InOrder(
				first,
				second,
			)
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			err := tw.RecoverForFailover(ctx)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_AddEvent(t *testing.T) {
	Convey("test timingwheel add event", t, func() {
		ctx := context.Background()
		e := event(2000)
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogWriter = mockEventlogWriter
			}
			next := e.Next()
			e = next
		}

		Convey("test timingwheel add event of expired", func() {
			mockEventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(1), nil)
			ret := tw.AddEvent(ctx, e)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheel_startReceivingStation(t *testing.T) {
	Convey("test timingwheel start receiving station", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.receivingStation.client.leaderClient = mockEventbusCtrlCli
		tw.receivingStation.kvStore = mockStoreCli

		Convey("test timingwheel start receiving station with create eventbus failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_RS",
			}).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_RS",
			}).Times(1).Return(nil, errors.New("test"))
			err := tw.startReceivingStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start receiving station with update offset metadata failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_RS",
			}).Times(1).Return(nil, nil)
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(errors.New("test"))
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			err := tw.startReceivingStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start receiving station with start failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_RS",
			}).Times(1).Return(nil, nil)
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(nil)
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			err := tw.startReceivingStation(ctx)
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_startDistributionStation(t *testing.T) {
	Convey("test timingwheel start distribution station", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.distributionStation.client.leaderClient = mockEventbusCtrlCli
		tw.distributionStation.kvStore = mockStoreCli

		Convey("test timingwheel start receiving station with create eventbus failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_DS",
			}).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_DS",
			}).Times(1).Return(nil, errors.New("test"))
			err := tw.startDistributionStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start receiving station with update offset metadata failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_DS",
			}).Times(1).Return(nil, nil)
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(errors.New("test"))
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			err := tw.startDistributionStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start receiving station with start failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_DS",
			}).Times(1).Return(nil, nil)
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(nil)
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			err := tw.startDistributionStation(ctx)
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_startTickingOfPointer(t *testing.T) {
	Convey("test timingwheel start ticking of pointer", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.config.Tick = 10 * time.Millisecond

		Convey("test timingwheel start ticking of pointer with ctx cancel", func() {
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
			tw.startTickingOfPointer(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel start ticking of pointer with ticker", func() {
			tw.SetLeader(true)
			go func() {
				for {
					<-tw.twList.Front().Value.(*timingWheelElement).tickC
				}
			}()
			go func() {
				time.Sleep(50 * time.Millisecond)
				cancel()
			}()
			tw.startTickingOfPointer(ctx)
			tw.wg.Wait()
		})
	})
}

func TestTimingWheel_startHeartBeat(t *testing.T) {
	Convey("test timingwheel start heartbeat", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())

		Convey("test timingwheel start heartbeat with ctx cancel", func() {
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.startHeartBeat(ctx)
		})
	})
}

func TestTimingWheel_startScheduledEventDispatcher(t *testing.T) {
	Convey("test timingwheel start scheduled event dispatcher", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		tw.receivingStation.eventlogReader = mockEventlogReader
		tw.receivingStation.eventlogWriter = mockEventlogWriter
		tw.receivingStation.kvStore = mockStoreCli

		Convey("test timingwheel start scheduled event dispatcher abnormal", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), es.ErrNoLeader)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.startScheduledEventDispatcher(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel start scheduled event dispatcher error no leader", func() {
			e := ce.NewEvent()
			e.SetExtension(xceVanusDeliveryTime, time.Now().UTC().Format(time.RFC3339))
			e.SetExtension(xceVanusEventbus, "quick-start")
			events := make([]*ce.Event, 1)
			events[0] = &e
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Eq(ctx), gomock.Any()).AnyTimes().Return(events, nil)
			ls := make([]*record.EventLog, 1)
			ls[0] = &record.EventLog{
				VRN: "testvrn",
			}
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), gomock.Any()).AnyTimes().Return(int64(1), errors.New("test"))
			mockEventlogWriter.EXPECT().Close().AnyTimes().Return()
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.startScheduledEventDispatcher(ctx)
			tw.wg.Wait()
		})
	})
}

func TestTimingWheel_calculateIndex(t *testing.T) {
	Convey("test timingwheel calculate index", t, func() {
		now := time.Now().UTC()
		tw := newtimingwheel(cfg())

		Convey("test timingwheel calculate index with layer equal 1", func() {
			twe := tw.twList.Front().Value.(*timingWheelElement)
			index := twe.calculateIndex(now.Add(1*time.Second), now)
			So(index, ShouldEqual, 1)
		})

		Convey("test timingwheel calculate index with layer equal 2", func() {
			twe := tw.twList.Front().Next().Value.(*timingWheelElement)
			index := twe.calculateIndex(now.Add(11*time.Second), now)
			So(index, ShouldEqual, 1)
		})
	})
}

func TestTimingWheel_resetBucketsCapacity(t *testing.T) {
	Convey("test timingwheel reset buckets capacity", t, func() {
		twe := newTimingWheelElement(cfg(), nil, nil, time.Second, 1)

		Convey("test timingwheel reset buckets capacity to 2", func() {
			twe.resetBucketsCapacity(2)
		})
	})
}

func TestTimingWheel_addEvent(t *testing.T) {
	Convey("test timingwheel add event", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogWriter = mockEventlogWriter
			}
			next := e.Next()
			e = next
		}

		Convey("test timingwheel add event to current layer", func() {
			e := event(2000)
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), nil)
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("test timingwheel add event to current layer return false", func() {
			e := event(2000)
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), errors.New("test"))
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, false)
		})

		Convey("test timingwheel add event to overflowwheel return false", func() {
			e := event(15000)
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), errors.New("test"))
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, false)
		})

		Convey("test timingwheel add event to highest layer return true", func() {
			e := event(1000)
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, gomock.Any()).Times(1).Return(nil, nil)
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), nil)
			ls := make([]*record.EventLog, 1)
			ls[0] = &record.EventLog{
				VRN: "testvrn",
			}
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, nil)
			defer stub3.Reset()
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Back().Value.(*timingWheelElement)
			twe.client.leaderClient = mockEventbusCtrlCli
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("test timingwheel add event to highest layer return false", func() {
			e := event(1000)
			mockEventbusCtrlCli.EXPECT().GetEventBus(ctx, gomock.Any()).Times(1).Return(nil, nil)
			mockEventlogWriter.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(-1), errors.New("test"))
			ls := make([]*record.EventLog, 1)
			ls[0] = &record.EventLog{
				VRN: "testvrn",
			}
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, nil, nil)
			defer stub3.Reset()
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Back().Value.(*timingWheelElement)
			twe.client.leaderClient = mockEventbusCtrlCli
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, false)
		})
	})
}

func TestTimingWheel_isExistBucket(t *testing.T) {
	Convey("test timingwheel is exist bucket", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())

		Convey("test timingwheel is exist with return false1", func() {
			ret := tw.twList.Back().Value.(*timingWheelElement).isExistBucket(ctx, 0)
			So(ret, ShouldBeFalse)
		})

		Convey("test timingwheel is exist with return false2", func() {
			tw.twList.Back().Value.(*timingWheelElement).resetBucketsCapacity(1)
			ret := tw.twList.Back().Value.(*timingWheelElement).isExistBucket(ctx, 0)
			So(ret, ShouldBeFalse)
		})

		Convey("test timingwheel is exist with return true", func() {
			tw.twList.Back().Value.(*timingWheelElement).resetBucketsCapacity(1)
			tw.twList.Back().Value.(*timingWheelElement).buckets[0] = newBucket(cfg(), nil, nil, 1, "__Timer_5_0", 1, 1)
			ret := tw.twList.Back().Value.(*timingWheelElement).isExistBucket(ctx, 0)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheel_fetchEventFromOverflowWheelAdvance(t *testing.T) {
	Convey("test timingwheel fetch event from overflowwheel advance", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = mockEventlogReader
			}
			next := e.Next()
			e = next
		}

		Convey("test timingwheel fetch event from overflowwheel advance1", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("test"))
			go func(tw *timingWheel) {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}(tw)
			tw.twList.Front().Value.(*timingWheelElement).fetchEventFromOverflowWheelAdvance(ctx)
			tw.twList.Front().Value.(*timingWheelElement).wg.Wait()
		})
	})
}

func TestTimingWheel_startScheduledEventDistributer(t *testing.T) {
	Convey("test timingwheel start scheduled event distributer", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = mockEventlogReader
				bucket.eventlogWriter = mockEventlogWriter
				bucket.kvStore = mockStoreCli
			}
			next := e.Next()
			e = next
		}
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test timingwheel start scheduled event distributer cancel1", func() {
			e := ce.NewEvent()
			events := []*ce.Event{&e}
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Eq(ctx), gomock.Any()).AnyTimes().Return(events, errors.New("test"))
			tw.twList.Front().Value.(*timingWheelElement).startScheduledEventDistributer(ctx)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			time.Sleep(200 * time.Millisecond)
			tw.Stop(ctx)
		})

		Convey("test timingwheel start scheduled event distributer cancel2", func() {
			e := ce.NewEvent()
			events := []*ce.Event{&e}
			mockEventlogReader.EXPECT().Seek(gomock.Eq(ctx), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Eq(ctx), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventlogWriter.EXPECT().Append(ctx, e).AnyTimes().Return(int64(0), nil)
			mockEventlogWriter.EXPECT().Close().AnyTimes().Return()
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			stub1 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&openLogWriter, mockEventlogWriter, nil)
			defer stub2.Reset()
			tw.twList.Front().Value.(*timingWheelElement).startScheduledEventDistributer(ctx)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			time.Sleep(200 * time.Millisecond)
			tw.Stop(ctx)
		})
	})
}

func TestTimingWheel_updatePointerMeta(t *testing.T) {
	Convey("test timingwheel update pointer metadata", t, func() {
		ctx := context.Background()
		twe := newTimingWheelElement(cfg(), nil, nil, time.Second, 1)
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		twe.kvStore = mockStoreCli

		Convey("test timingwheel update pointer metadata success", func() {
			key := "/vanus/internal/resource/timer/metadata/pointer/1"
			pointerMeta := &metadata.PointerMeta{
				Layer:   1,
				Pointer: 0,
			}
			data, _ := json.Marshal(pointerMeta)
			mockStoreCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			twe.updatePointerMeta(ctx)
			twe.wg.Wait()
		})

		Convey("test timingwheel update pointer metadata failure", func() {
			key := "/vanus/internal/resource/timer/metadata/pointer/1"
			pointerMeta := &metadata.PointerMeta{
				Layer:   1,
				Pointer: 0,
			}
			data, _ := json.Marshal(pointerMeta)
			mockStoreCli.EXPECT().Set(ctx, key, data).Times(1).Return(errors.New("test"))
			twe.updatePointerMeta(ctx)
			twe.wg.Wait()
		})
	})
}

func TestTimingWheel_startPointer(t *testing.T) {
	Convey("test timingwheel start pointer", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)

		Convey("test timingwheel start pointer cancel", func() {
			mockStoreCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(errors.New("test"))
			tw.twList.Front().Value.(*timingWheelElement).kvStore = mockStoreCli
			tw.twList.Front().Value.(*timingWheelElement).start(ctx)
			tw.twList.Front().Value.(*timingWheelElement).tickC <- struct{}{}
			time.Sleep(100 * time.Millisecond)
			cancel()
			tw.twList.Front().Value.(*timingWheelElement).wg.Wait()
		})
	})
}

func cfg() *Config {
	return &Config{
		CtrlEndpoints: []string{"127.0.0.1"},
		StartTime:     time.Now().UTC(),
		EtcdEndpoints: []string{"127.0.0.1"},
		KeyPrefix:     "/vanus",
		Layers:        4,
		Tick:          time.Second,
		WheelSize:     10,
	}
}

func newtimingwheel(c *Config) *timingWheel {
	l := list.New()
	cli := NewClient(c.CtrlEndpoints)
	// Init Hierarchical Timing Wheels.
	for layer := int64(1); layer <= c.Layers+1; layer++ {
		twe := newTimingWheelElement(c, nil, cli, exponent(time.Second, c.WheelSize, layer-1), layer)
		add(l, twe)
	}

	return &timingWheel{
		config:              c,
		client:              cli,
		twList:              l,
		leader:              false,
		receivingStation:    newBucket(cfg(), nil, cli, 1, "__Timer_RS", 1, 1),
		distributionStation: newBucket(cfg(), nil, cli, 1, "__Timer_DS", 1, 1),
		exitC:               make(chan struct{}),
	}
}
