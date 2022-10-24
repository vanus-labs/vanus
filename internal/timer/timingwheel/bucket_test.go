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
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/client"
	es "github.com/linkall-labs/vanus/client/pkg/errors"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	"github.com/linkall-labs/vanus/client/pkg/eventlog"
	"github.com/linkall-labs/vanus/client/pkg/record"
	"github.com/linkall-labs/vanus/internal/kv"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"

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
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := eventbus.NewMockEventbus(mockCtrl)
		mockBusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockBusReader := eventbus.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.client = mockClient
		bucket.ctrlCli = mockEventbusCtrlCli
		ls := make([]*record.Eventlog, 1)
		ls[0] = &record.Eventlog{
			ID: 1,
		}

		Convey("test bucket start with create eventbus failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
			err := bucket.start(ctx)
			So(err, ShouldNotBeNil)
		})

		// Convey("test bucket start with connect eventbus failed", func() {
		// 	mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
		// 	mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, nil)
		// 	err := bucket.start(ctx)
		// 	So(err, ShouldNotBeNil)
		// })

		Convey("test bucket start success", func() {
			bucket.timingwheel.leader = false
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
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := eventbus.NewMockEventbus(mockCtrl)
		mockEventlog := eventlog.NewMockEventlog(mockCtrl)
		mockBusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockBusReader := eventbus.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.eventbusReader = mockBusReader
		bucket.eventbusWriter = mockBusWriter
		bucket.kvStore = mockStoreCli
		bucket.client = mockClient
		tw.client = mockClient
		tw.distributionStation.eventbusWriter = mockBusWriter
		events := make([]*ce.Event, 1)
		events[0] = event(0)
		for e := tw.twList.Front(); e != nil; e = e.Next() {
			for _, bucket := range e.Value.(*timingWheelElement).buckets {
				bucket.eventbusWriter = mockBusWriter
				bucket.eventbusReader = mockBusReader
				bucket.timingwheel = tw
			}
		}

		Convey("get event failed", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), errors.New("test"))
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("get event on end", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), es.ErrOnEnd)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("push failed", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().AppendOne(Any(), Any()).AnyTimes().Return("", errors.New("test"))
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("flow failed", func() {
			events[0] = event(1000)
			bucket.layer = 2
			bucket.waitingForReady = bucket.waitingForFlow
			bucket.eventHandler = bucket.pushToPrevTimingWheel
			bucket.element = tw.twList.Front().Next()
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().AppendOne(Any(), Any()).AnyTimes().Return("", errors.New("test"))
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("flow success", func() {
			events[0] = event(1000)
			bucket.layer = 2
			bucket.waitingForReady = bucket.waitingForFlow
			bucket.eventHandler = bucket.pushToPrevTimingWheel
			bucket.element = tw.twList.Front().Next()
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().AppendOne(Any(), Any()).AnyTimes().Return("", nil)
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()
			bucket.run(ctx)
			bucket.wg.Wait()
		})

		Convey("push success", func() {
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), nil)
			mockBusWriter.EXPECT().AppendOne(Any(), Any()).AnyTimes().Return("", nil)
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).AnyTimes().Return(nil)
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
		mockCtrl := NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.ctrlCli = mockEventbusCtrlCli

		Convey("eventbus has exist", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, nil)
			err := bucket.createEventbus(ctx)
			So(err, ShouldBeNil)
		})

		Convey("create failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
			err := bucket.createEventbus(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("create success", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(Any(), Any()).Times(1).Return(nil, errors.New("test"))
			mockEventbusCtrlCli.EXPECT().CreateEventBus(Any(), Any()).Times(1).Return(nil, nil)
			err := bucket.createEventbus(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_deleteEventBus(t *testing.T) {
	Convey("test bucket delete eventbus", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		tw.SetLeader(true)
		bucket := newBucket(tw, nil, time.Second, "", 1, 0)
		bucket.timingwheel = tw
		mockCtrl := gomock.NewController(t)
		mockEventbusCtrlCli := ctrlpb.NewMockEventBusControllerClient(mockCtrl)
		bucket.client = mockEventbusCtrlCli

		Convey("eventbus has not exist", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			err := bucket.deleteEventbus(ctx)
			So(err, ShouldBeNil)
		})

		Convey("delete failed", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			mockEventbusCtrlCli.EXPECT().DeleteEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("test"))
			err := bucket.deleteEventbus(ctx)
			So(err, ShouldNotBeNil)
		})

		Convey("delete success", func() {
			mockEventbusCtrlCli.EXPECT().GetEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			mockEventbusCtrlCli.EXPECT().DeleteEventBus(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
			err := bucket.deleteEventbus(ctx)
			So(err, ShouldBeNil)
		})
	})
}

func TestBucket_connectEventbus(t *testing.T) {
	Convey("test bucket connect eventbus", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := eventbus.NewMockEventbus(mockCtrl)
		mockBusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockBusReader := eventbus.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		bucket.client = mockClient
		ls := make([]*record.Eventlog, 1)
		ls[0] = &record.Eventlog{
			ID: 1,
		}

		// Convey("test bucket connect eventbus with open eventbus writer failed", func() {
		// 	bucket.connectEventbus(ctx)
		// })

		// Convey("test bucket connect eventbus with lookup readable logs failed", func() {
		// 	err := bucket.connectEventbus(ctx)
		// 	So(err, ShouldNotBeNil)
		// })

		// Convey("test bucket connect eventbus with open eventlog reader failed", func() {
		// 	mockClient.EXPECT().OpenBusWriter(Any(), Any()).AnyTimes().Return(nil, nil)
		// 	mockClient.EXPECT().LookupReadableLogs(Any(), Any()).AnyTimes().Return(ls, nil)
		// 	mockClient.EXPECT().OpenLogReader(Any(), Any()).AnyTimes().Return(nil, errors.New("test"))
		// 	err := bucket.connectEventbus(ctx)
		// 	So(err, ShouldNotBeNil)
		// })

		Convey("test bucket connect eventbus success", func() {
			bucket.connectEventbus(ctx)
		})
	})
}

func TestBucket_disconnectEventbus(t *testing.T) {
	Convey("test bucket disconnect eventbus", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		bucket.eventbusWriter = mockEventbusWriter
		bucket.eventlogReader = mockEventlogReader

		Convey("test bucket disconnect eventbus success", func() {
			mockEventlogReader.EXPECT().Close(gomock.Any()).AnyTimes().Return()
			mockEventbusWriter.EXPECT().Close(gomock.Any()).AnyTimes().Return()
			bucket.disconnectEventbus(ctx)
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
		mockCtrl := NewController(t)
		mockClient := client.NewMockClient(mockCtrl)
		mockEventbus := eventbus.NewMockEventbus(mockCtrl)
		mockEventlog := eventlog.NewMockEventlog(mockCtrl)
		mockBusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockBusReader := eventbus.NewMockBusReader(mockCtrl)
		mockClient.EXPECT().Eventbus(Any(), Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		bucket.eventbusReader = mockBusReader
		bucket.client = mockClient

		Convey("test bucket get event success", func() {
			events := make([]*ce.Event, 1)
			events[0] = event(10000)
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), nil)
			result, err := bucket.getEvent(ctx, 1)
			So(len(result), ShouldEqual, 0)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket get event panic", func() {
			bucket.eventbusReader = nil
			tw.SetLeader(true)
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			result, err := bucket.getEvent(ctx, 1)
			So(len(result), ShouldEqual, 0)
			So(err, ShouldNotBeNil)
		})

		Convey("test bucket get event failed", func() {
			tw.SetLeader(true)
			events := make([]*ce.Event, 1)
			events[0] = event(10000)
			mockEventbus.EXPECT().ListLog(Any()).AnyTimes().Return([]eventlog.Eventlog{mockEventlog}, nil)
			mockBusReader.EXPECT().Read(Any(), Any(), Any()).AnyTimes().Return(events, int64(0), uint64(0), errors.New("test"))
			_, err := bucket.getEvent(ctx, 1)
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
		mockCtrl := NewController(t)
		mockStoreCli := kv.NewMockClient(mockCtrl)
		bucket.kvStore = mockStoreCli

		Convey("test bucket update offset metadata with follower", func() {
			tw.SetLeader(false)
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})

		Convey("test bucket update offset metadata success", func() {
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).Times(1).Return(nil)
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})

		Convey("test bucket update offset metadata failure", func() {
			mockStoreCli.EXPECT().Set(Any(), Any(), Any()).Times(1).Return(errors.New("test"))
			bucket.updateOffsetMeta(ctx, bucket.offset)
		})
	})
}

func TestBucket_hasOnEnd(t *testing.T) {
	Convey("test bucket on end", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)

		Convey("test bucket has on end", func() {
			ret := bucket.hasOnEnd(ctx)
			So(ret, ShouldBeFalse)
		})
	})
}

func TestBucket_recycle(t *testing.T) {
	Convey("test bucket recycle", t, func() {
		ctx := context.Background()
		tw := newtimingwheel(cfg())
		bucket := newBucket(tw, nil, 1, "", 1, 0)
		mockCtrl := gomock.NewController(t)
		mockEventbusWriter := eventbus.NewMockBusWriter(mockCtrl)
		mockEventlogReader := eventlog.NewMockLogReader(mockCtrl)
		bucket.eventbusWriter = mockEventbusWriter
		bucket.eventlogReader = mockEventlogReader

		Convey("test bucket wait success", func() {
			mockEventlogReader.EXPECT().Close(gomock.Any()).AnyTimes().Return()
			mockEventbusWriter.EXPECT().Close(gomock.Any()).AnyTimes().Return()
			bucket.recycle(ctx)
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
