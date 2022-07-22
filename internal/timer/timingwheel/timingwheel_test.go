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

// func TestTimingWheel_Init(t *testing.T) {
// 	Convey("test timingwheel init", t, func() {
// 		ctx := context.Background()
// 		tw := NewTimingWheel(cfg())
// 		Convey("test timingwheel init failure", func() {
// 			err := tw.Init(ctx)
// 			So(err, ShouldNotBeNil)
// 		})
// 	})
// }

func TestTimingWheel_Start(t *testing.T) {
	Convey("test timingwheel start", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		eventlogCtrl := eventlog.NewMockLogReader(mockCtrl)
		eventbusCtrl := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		// storeCtrl := kv.NewMockClient(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = eventlogCtrl
				bucket.client.leaderClient = eventbusCtrl
			}
			next := e.Next()
			e = next
		}
		ls := make([]*record.EventLog, 1)
		ls[0] = &record.EventLog{
			VRN: "testvrn",
		}

		Convey("test timingwheel start bucket create eventbus failure", func() {
			eventbusCtrl.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			eventbusCtrl.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			err := tw.Start(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel start bucket start failure", func() {
			eventbusCtrl.EXPECT().GetEventBus(ctx, &meta.EventBus{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, errors.New("test"))
			eventbusCtrl.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: "__Timer_1_0",
			}).Times(1).Return(nil, nil)
			stub1 := StubFunc(&lookupReadableLogs, ls, errors.New("test"))
			defer stub1.Reset()
			err := tw.Start(ctx)
			So(err, ShouldNotBeNil)
		})

		// Convey("test timingwheel start bucket start success", func() {
		// 	l := list.New()
		// 	tw.config.Layers = 1
		// 	tw.config.WheelSize = 1
		// 	tick := exponent(tw.config.Tick, tw.config.WheelSize, 0)
		// 	add(l, newTimingWheelElement(tw.config, tw.kvStore, tw.client, tick, 1))
		// 	tw.twList = l
		// 	cli := NewClient([]string{"127.0.0.1"})
		// 	tw.receivingStation = newBucket(cfg(), nil, cli, 1, "__Timer_RS", 1, 1)
		// 	tw.distributionStation = newBucket(cfg(), nil, cli, 1, "__Timer_DS", 1, 1)
		// 	tw.receivingStation.eventlogReader = eventlogCtrl
		// 	tw.distributionStation.eventlogReader = eventlogCtrl
		// 	tw.receivingStation.client.leaderClient = eventbusCtrl
		// 	tw.distributionStation.client.leaderClient = eventbusCtrl
		// 	tw.receivingStation.kvStore = storeCtrl
		// 	tw.distributionStation.kvStore = storeCtrl
		// 	key := "/vanus/internal/resource/timer/metadata/offset/__Timer_RS"
		// 	pointerMeta := &metadata.PointerMeta{
		// 		Layer:   1,
		// 		Pointer: 1,
		// 	}
		// 	data, _ := json.Marshal(pointerMeta)
		// 	eventbusCtrl.EXPECT().GetEventBus(ctx, &meta.EventBus{
		// 		Name: "__Timer_1_0",
		// 	}).AnyTimes().Return(nil, errors.New("test"))
		// 	eventbusCtrl.EXPECT().CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
		// 		Name: "__Timer_1_0",
		// 	}).AnyTimes().Return(nil, nil)
		// 	eventlogCtrl.EXPECT().Seek(ctx, int64(0), io.SeekStart).AnyTimes().Return(int64(0), errors.New("test"))
		// 	storeCtrl.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
		// 	stub1 := StubFunc(&lookupReadableLogs, ls, nil)
		// 	defer stub1.Reset()
		// 	stub2 := StubFunc(&openLogWriter, nil, nil)
		// 	defer stub2.Reset()
		// 	stub3 := StubFunc(&openLogReader, eventlogCtrl, nil)
		// 	defer stub3.Reset()
		// 	err := tw.Start(ctx)
		// 	So(err, ShouldBeNil)
		// })
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
		Convey("test timingwheel is not deployed", func() {
			tw.receivingStation = newBucket(cfg(), nil, nil, 1, "__Timer_1_0", 1, 1)
			stub1 := StubFunc(&lookupReadableLogs, nil, errors.New("test"))
			defer stub1.Reset()
			ret := tw.IsDeployed(ctx)
			So(ret, ShouldBeFalse)
		})
	})
}

func TestTimingWheel_RecoverForFailover(t *testing.T) {
	Convey("test timingwheel recover for failover", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		storeCtrl := kv.NewMockClient(mockCtrl)
		tw.kvStore = storeCtrl
		Convey("test timingwheel recover for failover failure", func() {
			key := "/vanus/internal/resource/timer/metadata/pointer"
			storeCtrl.EXPECT().List(ctx, key).Times(1).Return(nil, errors.New("test"))
			err := tw.RecoverForFailover(ctx)
			So(err, ShouldNotBeNil)
		})

		// Convey("test timingwheel recover for failover success", func() {
		// 	key := "/vanus/internal/resource/timer/metadata/pointer"
		// 	md := metadata.PointerMeta{
		// 		Layer:   1,
		// 		Pointer: 1,
		// 	}
		// 	data, _ := json.Marshal(md)
		// 	kvPairs := make([]kv.Pair, 1)
		// 	pair := kv.Pair{
		// 		Key:   "key",
		// 		Value: data,
		// 	}
		// 	kvPairs[0] = pair
		// 	storeCtrl.EXPECT().List(ctx, key).Times(1).Return(kvPairs, nil)
		// 	tw.twList = list.New()
		// 	err := tw.RecoverForFailover(ctx)
		// 	So(err, ShouldBeNil)
		// })
	})
}

func TestTimingWheel_AddEvent(t *testing.T) {
	Convey("test timingwheel add event", t, func() {
		ctx := context.Background()
		e := event(2000)
		tw := newtimingwheel(cfg())
		mockCtrl := gomock.NewController(t)
		eventlogWriter := eventlog.NewMockLogWriter(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogWriter = eventlogWriter
			}
			next := e.Next()
			e = next
		}
		Convey("test timingwheel add event of expired", func() {
			eventlogWriter.EXPECT().Append(ctx, e).Times(1).Return(int64(1), nil)
			ret := tw.AddEvent(ctx, e)
			So(ret, ShouldBeTrue)
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
		eventlogReader := eventlog.NewMockLogReader(mockCtrl)
		tw.receivingStation = newBucket(cfg(), nil, nil, 1, "__Timer_1_0", 1, 1)
		tw.receivingStation.eventlogReader = eventlogReader
		Convey("test timingwheel start scheduled event dispatcher abnormal", func() {
			e := event(1000)
			events := make([]*ce.Event, 1)
			events[0] = e
			eventlogReader.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), errors.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.startScheduledEventDispatcher(ctx)
		})
		Convey("test timingwheel start scheduled event dispatcher error no leader", func() {
			e := event(1000)
			events := make([]*ce.Event, 1)
			events[0] = e
			eventlogReader.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), nil)
			eventlogReader.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, es.ErrNoLeader)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.startScheduledEventDispatcher(ctx)
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
			So(index, ShouldEqual, 2)
		})
		Convey("test timingwheel calculate index with layer equal 2", func() {
			twe := tw.twList.Front().Next().Value.(*timingWheelElement)
			index := twe.calculateIndex(now.Add(1*time.Second), now)
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
		eventlogWriterCtrl := eventlog.NewMockLogWriter(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogWriter = eventlogWriterCtrl
			}
			next := e.Next()
			e = next
		}
		Convey("test timingwheel add event of current layer", func() {
			e := event(2000)
			eventlogWriterCtrl.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), nil)
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("test timingwheel add event of current layer return false", func() {
			e := event(2000)
			eventlogWriterCtrl.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), errors.New("test"))
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, false)
		})

		Convey("test timingwheel add event of overflowwheel", func() {
			e := event(15000)
			eventlogWriterCtrl.EXPECT().Append(gomock.Eq(ctx), e).Times(1).Return(int64(1), errors.New("test"))
			tm := newTimingMsg(ctx, e)
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.addEvent(ctx, tm)
			So(result, ShouldEqual, false)
		})
	})
}

// func TestTimingWheel_fetchEventFromOverflowWheelAdvance(t *testing.T) {
// 	Convey("test timingwheel fetch event from overflowwheel advance", t, func() {
// 		ctx := context.Background()
// 		e := event(1000)
// 		events := []*ce.Event{e}
// 		tw := newtimingwheel(cfg())
// 		tw.SetLeader(true)
// 		mockCtrl := gomock.NewController(t)
// 		eventlogCtrl := eventlog.NewMockLogReader(mockCtrl)
// 		for e := tw.twList.Front(); e != nil; {
// 			for _, bucket := range e.Value.(*timingWheelElement).buckets {
// 				bucket.eventlogReader = eventlogCtrl
// 			}
// 			next := e.Next()
// 			e = next
// 		}

// 		Convey("test timingwheel fetch event from overflowwheel advance1", func() {
// 			eventlogCtrl.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), nil)
// 			eventlogCtrl.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, errors.New("test"))
// 			add := func(context.Context, *timingMsg) bool {
// 				return true
// 			}
// 			tw.twList.Front().Value.(*timingWheelElement).config.WheelSize = 100
// 			tw.twList.Front().Value.(*timingWheelElement).pointer = 10
// 			go func(tw *timingWheel) {
// 				time.Sleep(200 * time.Millisecond)
// 				tw.twList.Front().Value.(*timingWheelElement).pointer += 2
// 			}(tw)
// 			tw.twList.Front().Value.(*timingWheelElement).fetchEventFromOverflowWheelAdvance(ctx, add)
// 		})

// 		Convey("test timingwheel fetch event from overflowwheel advance2", func() {
// 			eventlogCtrl.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), nil)
// 			eventlogCtrl.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, errors.New("test"))
// 			add := func(context.Context, *timingMsg) bool {
// 				return true
// 			}
// 			tw.twList.Front().Value.(*timingWheelElement).config.WheelSize = 1
// 			tw.twList.Front().Value.(*timingWheelElement).pointer = 0
// 			go func(tw *timingWheel) {
// 				time.Sleep(200 * time.Millisecond)
// 				tw.twList.Front().Value.(*timingWheelElement).pointer += 2
// 			}(tw)
// 			tw.twList.Front().Value.(*timingWheelElement).fetchEventFromOverflowWheelAdvance(ctx, add)
// 		})
// 	})
// }

func TestTimingWheel_startScheduledEventDistributer(t *testing.T) {
	Convey("test timingwheel start scheduled event distributer", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		eventlogCtrl := eventlog.NewMockLogReader(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = eventlogCtrl
			}
			next := e.Next()
			e = next
		}

		Convey("test timingwheel start scheduled event distributer cancel", func() {
			e := ce.NewEvent()
			events := []*ce.Event{&e}
			eventlogCtrl.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), nil)
			eventlogCtrl.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, errors.New("test"))
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
		storeCtrl := kv.NewMockClient(mockCtrl)
		twe.kvStore = storeCtrl
		Convey("test timingwheel update pointer metadata success", func() {
			key := "/vanus/internal/resource/timer/metadata/pointer/1"
			pointerMeta := &metadata.PointerMeta{
				Layer:   1,
				Pointer: 1,
			}
			data, _ := json.Marshal(pointerMeta)
			storeCtrl.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			err := twe.updatePointerMeta(ctx)
			So(err, ShouldBeNil)
		})

		Convey("test timingwheel update pointer metadata failure", func() {
			key := "/vanus/internal/resource/timer/metadata/pointer/1"
			pointerMeta := &metadata.PointerMeta{
				Layer:   1,
				Pointer: 1,
			}
			data, _ := json.Marshal(pointerMeta)
			storeCtrl.EXPECT().Set(ctx, key, data).Times(1).Return(errors.New("test"))
			err := twe.updatePointerMeta(ctx)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestTimingWheel_startPointer(t *testing.T) {
	Convey("test timingwheel start pointer", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := gomock.NewController(t)
		eventlogCtrl := eventlog.NewMockLogReader(mockCtrl)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventlogReader = eventlogCtrl
			}
			next := e.Next()
			e = next
		}

		Convey("test timingwheel start pointer cancel", func() {
			e := ce.NewEvent()
			events := []*ce.Event{&e}
			eventlogCtrl.EXPECT().Seek(gomock.Eq(ctx), int64(0), io.SeekStart).AnyTimes().Return(int64(0), nil)
			eventlogCtrl.EXPECT().Read(gomock.Eq(ctx), int16(1)).AnyTimes().Return(events, errors.New("test"))
			tw.twList.Front().Value.(*timingWheelElement).leader = true
			tw.twList.Front().Value.(*timingWheelElement).startPointerTimer(ctx)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			time.Sleep(200 * time.Millisecond)
			tw.Stop(ctx)
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
		tw := newTimingWheelElement(c, nil, cli, exponent(time.Second, c.WheelSize, layer-1), layer)
		add(l, tw)
	}

	return &timingWheel{
		config: c,
		client: cli,
		twList: l,
		leader: false,
		exitC:  make(chan struct{}),
	}
}
