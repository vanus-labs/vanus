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
	stderr "errors"
	"fmt"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/golang/mock/gomock"
	. "github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/api"
	"github.com/linkall-labs/vanus/client/pkg/record"
	"github.com/linkall-labs/vanus/pkg/cluster"
	"github.com/linkall-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/timer/metadata"
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
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockEventlog := api.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
		mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0), int64(0), uint64(0), nil)
		for e := tw.twList.Front(); e != nil; {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusReader = mockBusReader
				bucket.client = mockClient
				bucket.timingwheel = tw
			}
			next := e.Next()
			e = next
		}
		tw.client = mockClient
		tw.receivingStation.client = mockClient
		tw.distributionStation.client = mockClient
		tw.receivingStation.timingwheel = tw
		tw.distributionStation.timingwheel = tw
		ls := make([]*record.Eventlog, 1)
		ls[0] = &record.Eventlog{
			ID: 1,
		}

		mockCl := cluster.NewMockCluster(mockCtrl)
		tw.ctrl = mockCl
		mockSvc := cluster.NewMockEventbusService(mockCtrl)
		mockCl.EXPECT().EventbusService().AnyTimes().Return(mockSvc)
		mockSvc.EXPECT().CreateSystemEventbusIfNotExist(Any(), Any(), Any()).AnyTimes().Return(nil)
		mockSvc.EXPECT().IsExist(Any(), Any()).AnyTimes().Return(true)

		Convey("test timingwheel start bucket start success", func() {
			tw.SetLeader(false)
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
		mockCtrl := NewController(t)
		mockEventbusWriter := api.NewMockBusWriter(mockCtrl)
		mockEventbusReader := api.NewMockBusReader(mockCtrl)
		tw := newtimingwheel(cfg())
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).getBuckets() {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.eventbusReader = mockEventbusReader
			}
		}

		Convey("test timingwheel start receiving station with create eventbus failed", func() {
			tw.Stop(ctx)
		})
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
	Convey("test timingwheel is deployed", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockCl := cluster.NewMockCluster(mockCtrl)
		tw.ctrl = mockCl
		mockSvc := cluster.NewMockEventbusService(mockCtrl)
		mockCl.EXPECT().EventbusService().AnyTimes().Return(mockSvc)
		mockSvc.EXPECT().IsExist(Any(), Any()).AnyTimes().Return(true)

		Convey("test timingwheel is deployed", func() {
			ret := tw.IsDeployed(ctx)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheel_Recover(t *testing.T) {
	Convey("test timingwheel recover", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.kvStore = mockStoreCli

		Convey("test timingwheel recover with list failed", func() {
			mockStoreCli.EXPECT().List(Any(), Any()).Times(1).Return(nil, stderr.New("test"))
			err := tw.Recover(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel recover with no metadata", func() {
			mockStoreCli.EXPECT().List(Any(), Any()).Times(1).Return([]kv.Pair{}, nil)
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
			mockStoreCli.EXPECT().List(Any(), Any()).Times(1).Return(offsetKvPairs, nil)
			err := tw.Recover(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestTimingWheel_Push(t *testing.T) {
	Convey("test timingwheel push", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockEventbusWriter := api.NewMockBusWriter(mockCtrl)
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
		mockCtrl := NewController(t)
		mockCl := cluster.NewMockCluster(mockCtrl)
		tw.ctrl = mockCl
		mockSvc := cluster.NewMockEventbusService(mockCtrl)
		mockCl.EXPECT().EventbusService().AnyTimes().Return(mockSvc)

		Convey("test timingwheel start receiving station with create eventbus failed", func() {
			mockSvc.EXPECT().CreateSystemEventbusIfNotExist(Any(), Any(), Any()).Times(1).Return(errors.New("test"))
			err := tw.startReceivingStation(ctx)
			So(err, ShouldNotBeNil)
		})

		// Convey("test timingwheel start receiving station success", func() {
		// 	mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
		// 	mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, nil)
		// 	mockClient.EXPECT().EventbusService(Any(), Any()).AnyTimes().Return(mockEventbus)
		// 	mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		// 	mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		// 	err := tw.startReceivingStation(ctx)
		// 	So(err, ShouldBeNil)
		// })
	})
}

func TestTimingWheel_runReceivingStation(t *testing.T) {
	Convey("test timingwheel run receiving station", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockEventlog := api.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		tw.receivingStation.client = mockClient
		tw.receivingStation.eventbusReader = mockBusReader
		tw.receivingStation.eventbusWriter = mockBusWriter
		tw.distributionStation.client = mockClient
		tw.distributionStation.eventbusWriter = mockBusWriter
		tw.receivingStation.kvStore = mockStoreCli
		tw.receivingStation.timingwheel = tw
		tw.distributionStation.timingwheel = tw
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)

		Convey("test bucket run receiving station with get event failed", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0),
				int64(0), uint64(0), stderr.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runReceivingStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run receiving station with start failure", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0), int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, stderr.New("test"))
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runReceivingStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run receiving station with start success", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0), int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, nil)
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
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
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		// mockEventlog := eventlog.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		tw.client = mockClient
		tw.ctrlCli = mockEventbusCtrlCli
		tw.receivingStation.client = mockClient
		tw.distributionStation.client = mockClient
		mockCl := cluster.NewMockCluster(mockCtrl)
		tw.ctrl = mockCl
		mockSvc := cluster.NewMockEventbusService(mockCtrl)
		mockCl.EXPECT().EventbusService().AnyTimes().Return(mockSvc)

		Convey("test timingwheel start distribution station with create eventbus failed", func() {
			mockSvc.EXPECT().CreateSystemEventbusIfNotExist(Any(), Any(), Any()).Times(1).Return(errors.New("test"))
			err := tw.startDistributionStation(ctx)
			So(err, ShouldNotBeNil)
		})

		// Convey("test timingwheel start distribution station success", func() {
		// 	mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
		// 	mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, nil)
		// 	mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
		// 	mockClient.EXPECT().EventbusService(Any(), Any()).AnyTimes().Return(mockEventbus)
		// 	mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		// 	err := tw.startDistributionStation(ctx)
		// 	So(err, ShouldBeNil)
		// })
	})
}

func TestTimingWheel_runDistributionStation(t *testing.T) {
	Convey("test timingwheel run distribution station", t, func() {
		ctx, cancel := context.WithCancel(context.Background())
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockEventlog := api.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		tw.client = mockClient
		tw.distributionStation.eventbusReader = mockBusReader
		tw.distributionStation.kvStore = mockStoreCli
		tw.distributionStation.timingwheel = tw
		tw.distributionStation.client = mockClient

		Convey("test timingwheel run distribution station with get event failed", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0),
				int64(0), uint64(0), stderr.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runDistributionStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run distribution station with deliver failed", func() {
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0), int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, errors.ErrNotWritable)
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runDistributionStation(ctx)
			tw.wg.Wait()
		})

		Convey("test timingwheel run distribution station with deliver success", func() {
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(batch(0), int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, nil)
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]api.Eventlog{mockEventlog}, nil)
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			tw.runDistributionStation(ctx)
			tw.wg.Wait()
		})
	})
}

func TestTimingWheel_deliver(t *testing.T) {
	Convey("test timingwheel deliver", t, func() {
		ctx := context.Background()
		e := event(2000)
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		tw.client = mockClient

		Convey("test timingwheel deliver failure with abnormal event", func() {
			e.SetExtension(xVanusEventbus, time.Now())
			err := tw.deliver(ctx, e)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel deliver failure with eventbus not found", func() {
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, errors.ErrOffsetOnEnd)
			err := tw.deliver(ctx, e)
			So(err, ShouldBeNil)
		})

		Convey("test timingwheel deliver failure with append failed", func() {
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, errors.ErrNotWritable)
			err := tw.deliver(ctx, e)
			So(err, ShouldNotBeNil)
		})

		Convey("test timingwheel deliver success", func() {
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, nil)
			err := tw.deliver(ctx, e)
			So(err, ShouldBeNil)
		})
	})
}

func TestTimingWheelElement_push(t *testing.T) {
	Convey("test timingwheelelement push", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockBusWriter
				bucket.timingwheel = tw
			}
		}

		Convey("push timing message to overflow wheel", func() {
			tm := newTimingMsg(ctx, event(33000))
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.push(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("push timing message to timingwheel success", func() {
			tm := newTimingMsg(ctx, event(1000))
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.push(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("push timing message to timingwheel failure", func() {
			tw.SetLeader(true)
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, stderr.New("test"))
			tm := newTimingMsg(ctx, event(1000))
			twe := tw.twList.Front().Value.(*timingWheelElement)
			result := twe.push(ctx, tm)
			So(result, ShouldEqual, false)
		})
	})
}

func TestTimingWheelElement_pushBack(t *testing.T) {
	Convey("test timingwheel element push back", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		// mockEventlog := eventlog.NewMockEventlog(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)

		tw.client = mockClient
		ls := make([]*record.Eventlog, 1)
		ls[0] = &record.Eventlog{
			ID: 0,
		}
		mockCl := cluster.NewMockCluster(mockCtrl)
		tw.ctrl = mockCl
		mockSvc := cluster.NewMockEventbusService(mockCtrl)
		mockCl.EXPECT().EventbusService().AnyTimes().Return(mockSvc)

		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockBusWriter
				bucket.eventbusReader = mockBusReader
				bucket.timingwheel = tw
				bucket.kvStore = mockStoreCli
			}
		}

		// Convey("push timing message failure causes bucket create failed", func() {
		// 	tm := newTimingMsg(ctx, event(1000))
		// 	twe := tw.twList.Back().Value.(*timingWheelElement)
		// 	result := twe.pushBack(ctx, tm)
		// 	So(result, ShouldEqual, true)
		// })

		// Convey("push timing message failure causes append failed", func() {
		// 	tw.SetLeader(true)
		// 	mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
		// 	mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, nil)
		// 	mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
		// 	mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return([]*ce.Event{}, int64(0), uint64(0), es.ErrOnEnd)
		// 	mockBusWriter.EXPECT().AppendOne(Any(), Any()).AnyTimes().Return("", errors.New("test"))
		// 	tm := newTimingMsg(ctx, event(1000))
		// 	twe := tw.twList.Back().Value.(*timingWheelElement)
		// 	result := twe.pushBack(ctx, tm)
		// 	So(result, ShouldEqual, false)
		// })

		Convey("push timing message failure causes start failed", func() {
			tw.SetLeader(true)
			mockSvc.EXPECT().CreateSystemEventbusIfNotExist(Any(), Any(), Any()).Times(1).Return(errors.New("test"))
			tm := newTimingMsg(ctx, event(1000))
			twe := tw.twList.Back().Value.(*timingWheelElement)
			result := twe.pushBack(ctx, tm)
			So(result, ShouldEqual, false)
		})
	})
}

func TestTimingWheelElement_allowPush(t *testing.T) {
	Convey("test timingwheelelement allow push", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())

		Convey("test timingwheelelement calculateIndex with highest layer", func() {
			tm := newTimingMsg(ctx, event(1000))
			ret := tw.twList.Front().Value.(*timingWheelElement).allowPush(tm)
			So(ret, ShouldBeTrue)
		})
	})
}

func TestTimingWheelElement_flow(t *testing.T) {
	Convey("test timingwheelelement flow", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := api.NewMockEventbus(mockCtrl)
		mockBusWriter := api.NewMockBusWriter(mockCtrl)
		mockBusReader := api.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockBusWriter
				bucket.timingwheel = tw
			}
		}

		Convey("flow timing message failure causes append failed", func() {
			tw.SetLeader(true)
			tm := newTimingMsg(ctx, event(1000))
			mockBusWriter.EXPECT().Append(Any(), Any()).AnyTimes().Return([]string{""}, stderr.New("test"))
			result := tw.twList.Front().Value.(*timingWheelElement).flow(ctx, tm)
			So(result, ShouldEqual, false)
		})

		Convey("flow timing message success", func() {
			tm := newTimingMsg(ctx, event(1000))
			result := tw.twList.Front().Value.(*timingWheelElement).flow(ctx, tm)
			So(result, ShouldEqual, true)
		})

		Convey("flow timing message to buffer success", func() {
			tm := newTimingMsg(ctx, event(1000))
			result := tw.twList.Front().Value.(*timingWheelElement).flow(ctx, tm)
			So(result, ShouldEqual, true)
		})
	})
}

// func TestTimingWheelElement_calculateIndex(t *testing.T) {
// 	Convey("test timingwheelelement calculateIndex", t, func() {
// 		ctx := context.Background()
// 		tw := newtimingwheel(cfg())

// 		Convey("test timingwheelelement calculateIndex with bucket", func() {
// 			tm := newTimingMsg(ctx, event(1000))
// 			ret := tw.twList.Front().Value.(*timingWheelElement).calculateIndex(tm)
// 			So(ret, ShouldBeLessThan, 10)
// 		})

// 		Convey("test timingwheelelement calculateIndex with buffer", func() {
// 			tm := newTimingMsg(ctx, event(1000))
// 			ret := tw.twList.Back().Value.(*timingWheelElement).calculateIndex(tm)
// 			So(ret, ShouldEqual, 0)
// 		})
// 	})
// }

func TestTimingWheelElement_makeSureBucketExist(t *testing.T) {
	Convey("test timingwheelelement make sure bucket exist", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		mockCtrl := NewController(t)
		mockEventbusWriter := api.NewMockBusWriter(mockCtrl)
		mockEventbusReader := api.NewMockBusReader(mockCtrl)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		tw.ctrlCli = mockEventbusCtrlCli
		events := make([]*ce.Event, 1)
		events[0] = event(0)
		ls := make([]*record.Eventlog, 1)
		ls[0] = &record.Eventlog{
			ID: 1,
		}
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockEventbusWriter
				bucket.eventbusReader = mockEventbusReader
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
			buckets := make(map[int64]*bucket, c.WheelSize+defaultNumberOfTickFlowInAdvance)
			for i := int64(0); i < c.WheelSize+defaultNumberOfTickFlowInAdvance; i++ {
				ebName := fmt.Sprintf(timerBuiltInEventbus, layer, i)
				buckets[i] = newBucket(timingWheelInstance, twe.element, tick, ebName, layer, i)
			}
			twe.buckets = buckets
		} else {
			twe.buckets = make(map[int64]*bucket)
		}
		timingWheelInstance.receivingStation =
			newBucket(timingWheelInstance, nil, 0, timerBuiltInEventbusReceivingStation, 0, 0)
		timingWheelInstance.distributionStation =
			newBucket(timingWheelInstance, nil, 0, timerBuiltInEventbusDistributionStation, 0, 0)
	}
	return timingWheelInstance
}
