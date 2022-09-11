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
	"fmt"
	"io"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	errcli "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	eventlog "github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
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
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = mockEventlogReader
				bucket.client = mockEventbusCtrlCli
				bucket.timingwheel = tw
			}
			next := e.Next()
			e = next
		}
		tw.receivingStation.client = mockEventbusCtrlCli
		tw.distributionStation.client = mockEventbusCtrlCli
		tw.receivingStation.timingwheel = tw
		tw.distributionStation.timingwheel = tw
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test timingwheel start bucket with start failure", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("test"))
			err := tw.Start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start bucket start success", func() {
			tw.SetLeader(false)
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("test"))
			stub1 := StubFunc(&openBusWriter, nil, nil)
			defer stub1.Reset()
			stub2 := StubFunc(&lookupReadableLogs, ls, nil)
			defer stub2.Reset()
			stub3 := StubFunc(&openLogReader, mockEventlogReader, nil)
			defer stub3.Reset()
			err := tw.Start(ctx)
			cancel()
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
			leader: false,
		}
		tw.SetLeader(true)
		So(tw.leader, ShouldBeTrue)
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
		tw.receivingStation.client = mockEventbusCtrlCli
		tw.distributionStation.client = mockEventbusCtrlCli

		Convey("test timingwheel is not deployed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)
			ret := tw.IsDeployed(ctx)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheel_Recover(t *testing.T) {
	Convey("test timingwheel recover", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.kvStore = mockStoreCli

		Convey("test timingwheel recover with list failed", func() {
			mockStoreCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			err := tw.Recover(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel recover with no metadata", func() {
			mockStoreCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return([]kv.Pair{}, nil)
			err := tw.Recover(ctx)
			So(err, ShouldBeNil)
		})

		Convey("test timingwheel recover success", func() {
			data, _ := json.Marshal(metadata.OffsetMeta{
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
			mockStoreCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return(offsetKvPairs, nil)
			err := tw.Recover(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestTimingWheel_Push(t *testing.T) {
	Convey("test timingwheel push", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.timingwheel = tw
			}
			next := e.Next()
			e = next
		}
		tw.distributionStation.eventbusWriter = mockEventbusWriter
		tw.distributionStation.timingwheel = tw

		Convey("test timingwheel push event which has expired", func() {
			e := event(0)
			ret := tw.Push(ctx, e)
			So(ret, ShouldBeTrue)
		})

		Convey("test timingwheel push event success", func() {
			e := event(1000)
			ret := tw.Push(ctx, e)
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
		tw.receivingStation.client = mockEventbusCtrlCli

		Convey("test timingwheel start receiving station with create eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			err := tw.startReceivingStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start receiving station with connect eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			stubs := StubFunc(&openBusWriter, nil, errors.New("test"))
			defer stubs.Reset()
			err := tw.startReceivingStation(ctx)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_runReceivingStation(t *testing.T) {
	Convey("test timingwheel run receiving station", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.receivingStation.eventlogReader = mockEventlogReader
		tw.receivingStation.eventbusWriter = mockEventbusWriter
		tw.distributionStation.eventbusWriter = mockEventbusWriter
		tw.receivingStation.kvStore = mockStoreCli
		tw.receivingStation.timingwheel = tw
		tw.distributionStation.timingwheel = tw
		events := make([]*ce.Event, 1)
		events[0] = event(0)

		Convey("test bucket run receiving station with get event failed", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, errors.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runReceivingStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run receiving station with start failure", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", errors.New("test"))
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runReceivingStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run receiving station with start success", func() {
			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", nil)
			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runReceivingStation(ctx)
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
		tw.client = mockEventbusCtrlCli
		tw.receivingStation.client = mockEventbusCtrlCli

		Convey("test timingwheel start distribution station with create eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			tw.distributionStation.client = mockEventbusCtrlCli
			err := tw.startDistributionStation(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start distribution station with connect eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			stubs := StubFunc(&openBusWriter, nil, errors.New("test"))
			defer stubs.Reset()
			tw.distributionStation.client = mockEventbusCtrlCli
			err := tw.startDistributionStation(ctx)
			So(err, ShouldNotBeNil)
		})
	})
}

// func TestTimingWheel_runDistributionStation(t *testing.T) {
// 	Convey("test timingwheel run distribution station", t, func() {
// 		ctx, cancel := context.WithCancel(context.Background())
// 		tw := newtimingwheel(cfg())
// 		tw.SetLeader(true)
// 		mockCtrl := gomock.NewController(t)
// 		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
// 		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
// 		mockStoreCli := kv.NewMockClient(mockCtrl)
// 		tw.distributionStation.eventlogReader = mockEventlogReader
// 		tw.distributionStation.kvStore = mockStoreCli
// 		tw.distributionStation.timingwheel = tw
// 		events := make([]*ce.Event, 1)
// 		events[0] = event(0)

// 		Convey("test timingwheel run distribution station with get event failed", func() {
// 			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
// 			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, errors.New("test"))
// 			go func() {
// 				time.Sleep(100 * time.Millisecond)
// 				cancel()
// 			}()
// 			tw.runDistributionStation(ctx)
// 			tw.wg.Wait()
// 		})

// 		Convey("test timingwheel run distribution station with pop failed", func() {
// 			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
// 			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
// 			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
// 			stubs := StubFunc(&openBusWriter, mockEventbusWriter, errors.New("test"))
// 			defer stubs.Reset()
// 			go func() {
// 				time.Sleep(100 * time.Millisecond)
// 				cancel()
// 			}()
// 			tw.runDistributionStation(ctx)
// 			tw.wg.Wait()
// 		})

// 		Convey("test timingwheel run distribution station with pop success", func() {
// 			mockEventlogReader.EXPECT().Seek(gomock.Any(), gomock.Any(), io.SeekStart).AnyTimes().Return(int64(0), nil)
// 			mockEventlogReader.EXPECT().Read(gomock.Any(), gomock.Any()).AnyTimes().Return(events, nil)
// 			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().Return("", nil)
// 			mockEventbusWriter.EXPECT().Close().AnyTimes().Return()
// 			mockStoreCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
// 			stubs := StubFunc(&openBusWriter, mockEventbusWriter, nil)
// 			defer stubs.Reset()
// 			go func() {
// 				time.Sleep(100 * time.Millisecond)
// 				cancel()
// 			}()
// 			tw.runDistributionStation(ctx)
// 			tw.wg.Wait()
// 		})
// 	})
// }

func TestTimingWheel_deliver(t *testing.T) {
	Convey("test timingwheel deliver", t, func() {
		ctx := context.Background()
		e := event(2000)
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)

		Convey("test timingwheel deliver failure with abnormal event", func() {
			e.SetExtension(xVanusEventbus, time.Now())
			err := tw.deliver(ctx, e)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel deliver failure with open eventbus writer failed", func() {
			stubs := StubFunc(&openBusWriter, mockEventbusWriter, errors.New("test"))
			defer stubs.Reset()
			err := tw.deliver(ctx, e)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel deliver failure with eventbus not found", func() {
			stubs := StubFunc(&openBusWriter, mockEventbusWriter, nil)
			defer stubs.Reset()
			mockEventbusWriter.EXPECT().Append(ctx, gomock.Any()).Times(1).Return("", errcli.ErrNotFound)
			mockEventbusWriter.EXPECT().Close(context.Background()).Times(1).Return()
			err := tw.deliver(ctx, e)
			So(err, ShouldBeNil)
		})

		Convey("test timingwheel deliver failure with append failed", func() {
			stubs := StubFunc(&openBusWriter, mockEventbusWriter, nil)
			defer stubs.Reset()
			mockEventbusWriter.EXPECT().Append(ctx, gomock.Any()).Times(1).Return("", errors.New("test"))
			mockEventbusWriter.EXPECT().Close(context.Background()).Times(1).Return()
			err := tw.deliver(ctx, e)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel deliver success", func() {
			stubs := StubFunc(&openBusWriter, mockEventbusWriter, nil)
			defer stubs.Reset()
			mockEventbusWriter.EXPECT().Append(ctx, gomock.Any()).Times(1).Return("", nil)
			mockEventbusWriter.EXPECT().Close(context.Background()).Times(1).Return()
			err := tw.deliver(ctx, e)
			So(err, ShouldBeNil)
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
			tw.wg.Wait()
		})
	})
}

func TestTimingWheelElement_push(t *testing.T) {
	Convey("test timingwheelelement push timing message", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		tw.client = mockEventbusCtrlCli

		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.timingwheel = tw
			}
		}

		Convey("test timingwheelelement push timing message failure with bucket not exist", func() {
			e := event(2000)
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			tm := newTimingMsg(ctx, e)
			result := tw.twList.Back().Value.(*timingWheelElement).push(ctx, tm, true)
			So(result, ShouldEqual, false)
		})

		Convey("test timingwheelelement push timing message failure", func() {
			e := event(2000)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).Times(1).Return("", errors.New("test"))
			tm := newTimingMsg(ctx, e)
			result := tw.twList.Front().Value.(*timingWheelElement).push(ctx, tm, true)
			So(result, ShouldEqual, false)
		})

		Convey("test timingwheelelement push timing message success", func() {
			e := event(33000)
			mockEventbusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).Times(1).Return("", nil)
			tm := newTimingMsg(ctx, e)
			result := tw.twList.Front().Value.(*timingWheelElement).push(ctx, tm, false)
			So(result, ShouldEqual, true)
		})
	})
}

func TestTimingWheelElement_calculateIndex(t *testing.T) {
	Convey("test timingwheelelement calculateIndex", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())

		Convey("test timingwheelelement calculateIndex with highest layer", func() {
			tm := newTimingMsg(ctx, event(1000))
			ret := tw.twList.Back().Value.(*timingWheelElement).calculateIndex(tm, false)
			So(ret, ShouldBeGreaterThan, 32)
		})
	})
}

func TestTimingWheelElement_makeSureBucketExist(t *testing.T) {
	Convey("test timingwheelelement make sure bucket exist", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		tw.client = mockEventbusCtrlCli
		events := make([]*ce.Event, 1)
		events[0] = event(0)
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.eventlogReader = mockEventlogReader
				bucket.kvStore = mockStoreCli
				bucket.timingwheel = tw
			}
		}

		Convey("test timingwheelelement make sure bucket exist with return false1", func() {
			ret := tw.twList.Front().Value.(*timingWheelElement).makeSureBucketExist(ctx, 0)
			So(ret, ShouldBeNil)
		})
	})
}

func cfg() *Config {
	return &Config{
		CtrlEndpoints: []string{"127.0.0.1"},
		EtcdEndpoints: []string{"127.0.0.1"},
		KeyPrefix:     "/vanus",
		Layers:        4,
		Tick:          time.Second,
		WheelSize:     10,
	}
}

func newtimingwheel(c *Config) *timingWheel {
	timingWheelInstance := &timingWheel{
		config: c,
		twList: list.New(),
		leader: false,
		exitC:  make(chan struct{}),
	}

	for layer := int64(1); layer <= c.Layers+1; layer++ {
		tick := exponent(time.Second, c.WheelSize, layer-1)
		twe := newTimingWheelElement(timingWheelInstance, tick, layer)
		twe.setElement(timingWheelInstance.twList.PushBack(twe))
		if layer <= c.Layers {
			buckets := make(map[int64]*bucket, c.WheelSize+defaultNumberOfTickLoadsInAdvance)
			for i := int64(0); i < c.WheelSize+defaultNumberOfTickLoadsInAdvance; i++ {
				ebName := fmt.Sprintf(timerBuiltInEventbus, layer, i)
				buckets[i] = newBucket(timingWheelInstance, twe.element, tick, ebName, layer, i)
			}
			twe.buckets = buckets
		}
		timingWheelInstance.receivingStation = newBucket(timingWheelInstance, nil, 0, timerBuiltInEventbusReceivingStation, 0, 0)
		timingWheelInstance.distributionStation = newBucket(timingWheelInstance, nil, 0, timerBuiltInEventbusDistributionStation, 0, 0)
	}
	return timingWheelInstance
}
