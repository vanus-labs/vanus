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

package trigger

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/eventbus"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/client"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTrigger_Options(t *testing.T) {
	Convey("test trigger option", t, func() {
		tg := &trigger{}
		WithFilterProcessSize(0)(tg)
		So(tg.config.FilterProcessSize, ShouldEqual, 0)
		size := rand.Intn(1000) + 1
		WithFilterProcessSize(size)(tg)
		So(tg.config.FilterProcessSize, ShouldEqual, size)
		WithDeliveryTimeout(0)(tg)
		So(tg.config.DeliveryTimeout, ShouldEqual, defaultDeliveryTimeout)
		size = rand.Intn(1000) + size
		WithDeliveryTimeout(int32(size))(tg)
		So(tg.config.DeliveryTimeout, ShouldEqual, time.Duration(size)*time.Millisecond)
		WithMaxRetryAttempts(0)(tg)
		So(tg.config.MaxRetryAttempts, ShouldEqual, primitive.MaxRetryAttempts)
		size = rand.Intn(1000) + size
		WithMaxRetryAttempts(int32(size))(tg)
		So(tg.config.MaxRetryAttempts, ShouldEqual, size)
		WithBufferSize(0)(tg)
		So(tg.config.BufferSize, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithBufferSize(size)(tg)
		So(tg.config.BufferSize, ShouldEqual, size)
		WithRateLimit(0)(tg)
		So(tg.config.RateLimit, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithRateLimit(int32(size))(tg)
		So(tg.config.RateLimit, ShouldEqual, size)
		WithDeadLetterEventbus("")(tg)
		So(tg.config.DeadLetterEventbus, ShouldEqual, primitive.DeadLetterEventbusName)
		WithDeadLetterEventbus("test_eb")(tg)
		So(tg.config.DeadLetterEventbus, ShouldEqual, "test_eb")
	})
}

func TestTriggerStartStop(t *testing.T) {
	Convey("test start and stop", t, func() {
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r := reader.NewMockReader(ctrl)
		r2 := reader.NewMockReader(ctrl)
		ctx := context.Background()
		busWriter := eventbus.NewMockBusWriter(ctrl)
		stub := gostub.Stub(&eb.OpenBusWriter, func(_ context.Context, _ string) (eventbus.BusWriter, error) {
			return busWriter, nil
		})
		defer stub.Reset()
		busWriter.EXPECT().Close(context.Background()).Times(2).Return()
		err := tg.Init(ctx)
		So(err, ShouldBeNil)
		tg.reader = r
		tg.retryEventReader = r2
		r.EXPECT().Start().Return(nil)
		r2.EXPECT().Start().Return(nil)
		err = tg.Start(ctx)
		So(err, ShouldBeNil)
		time.Sleep(100 * time.Millisecond)
		So(tg.state, ShouldEqual, TriggerRunning)
		r.EXPECT().Close().Return()
		r2.EXPECT().Close().Return()
		_ = tg.Stop(ctx)
		So(tg.state, ShouldEqual, TriggerStopped)
	})
}

func TestTriggerWriteFailEvent(t *testing.T) {
	Convey("test write fail event", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		busWriter := eventbus.NewMockBusWriter(ctrl)
		stub := gostub.Stub(&eb.OpenBusWriter, func(_ context.Context, _ string) (eventbus.BusWriter, error) {
			return busWriter, nil
		})
		defer stub.Reset()
		_ = tg.Init(ctx)
		e := makeEventRecord("type")
		var callCount int
		busWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(context.Context,
			*ce.Event) (int64, error) {
			callCount++
			if callCount%2 != 0 {
				return int64(-1), fmt.Errorf("append error")
			}
			return int64(1), nil
		})
		Convey("test no need retry,in dlq", func() {
			tg.writeFailEvent(ctx, e.Event, 400, fmt.Errorf("400 error"))
			So(e.Event.Extensions()[primitive.DeadLetterReason], ShouldNotBeNil)
		})
		Convey("test first retry,in retry", func() {
			tg.writeFailEvent(ctx, e.Event, 500, fmt.Errorf("500 error"))
			So(e.Event.Extensions()[primitive.XVanusRetryAttempts], ShouldEqual, 1)
			So(e.Event.Extensions()[primitive.XVanusEventbus], ShouldEqual, primitive.RetryEventbusName)
			So(e.Event.Extensions()[primitive.XVanusSubscriptionID], ShouldEqual, id.String())
		})
		Convey("test retry again,in retry", func() {
			attempts := 1
			e.Event.SetExtension(primitive.XVanusRetryAttempts, strconv.Itoa(attempts))
			e.Event.SetExtension(primitive.XVanusEventbus, primitive.RetryEventbusName)
			tg.writeFailEvent(ctx, e.Event, 500, fmt.Errorf("500 error"))
			So(e.Event.Extensions()[primitive.XVanusRetryAttempts], ShouldEqual, attempts+1)
			So(e.Event.Extensions()[primitive.XVanusEventbus], ShouldEqual, primitive.RetryEventbusName)
		})
		Convey("test attempts max,in dlq", func() {
			attempts := primitive.MaxRetryAttempts
			e.Event.SetExtension(primitive.XVanusRetryAttempts, strconv.Itoa(attempts))
			tg.writeFailEvent(ctx, e.Event, 500, fmt.Errorf("500 error"))
			So(e.Event.Extensions()[primitive.XVanusRetryAttempts], ShouldEqual, strconv.Itoa(attempts))
			So(e.Event.Extensions()[primitive.DeadLetterReason], ShouldNotBeNil)
		})
	})
}

func TestTriggerRunEventSend(t *testing.T) {
	Convey("test event run process", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		cli := client.NewMockEventClient(ctrl)
		ctx := context.Background()
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		busWriter := eventbus.NewMockBusWriter(ctrl)
		stub := gostub.Stub(&eb.OpenBusWriter, func(ctx context.Context, _ string) (eventbus.BusWriter, error) {
			return busWriter, nil
		})
		defer stub.Reset()
		_ = tg.Init(ctx)
		tg.client = cli
		cli.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().Return(client.Success)
		size := 10
		for i := 0; i < size; i++ {
			_ = tg.eventArrived(ctx, makeEventRecord("test"))
		}
		So(len(tg.eventCh), ShouldEqual, size)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			tg.runEventFilter(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
		So(len(tg.sendCh), ShouldEqual, size)
		_ = tg.eventArrived(ctx, makeEventRecord("no"))
		time.Sleep(100 * time.Millisecond)
		So(len(tg.sendCh), ShouldEqual, size)
		close(tg.eventCh)
		wg.Wait()
		wg.Add(1)
		go func() {
			defer wg.Done()
			tg.runEventSend(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
		close(tg.sendCh)
		wg.Wait()
	})
}

func TestTriggerRateLimit(t *testing.T) {
	Convey("test rate limit", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		cli := client.NewMockEventClient(ctrl)
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		tg.client = cli
		cli.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().Return(client.Success)
		rateLimit := int32(10000)
		Convey("test no rate limit", func() {
			c := testSendEvent(tg)
			So(c, ShouldBeGreaterThan, rateLimit)
		})
		Convey("test with rate", func() {
			WithRateLimit(rateLimit)(tg)
			c := testSendEvent(tg)
			So(c, ShouldBeLessThanOrEqualTo, 1*rateLimit+10)
		})
	})
}

func testSendEvent(tg *trigger) int64 {
	size := 50000
	eventCh := make(chan *ce.Event, size)
	for i := 0; i < size; i++ {
		eventCh <- makeEventRecord("test").Event
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var c int64
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventCh:
				if !ok {
					return
				}
				_, _ = tg.sendEvent(ctx, event)
				atomic.AddInt64(&c, 1)
			}
		}
	}()
	time.Sleep(1 * time.Second)
	close(eventCh)
	cancel()
	wg.Wait()
	return c
}

func makeSubscription(id vanus.ID) *primitive.Subscription {
	return &primitive.Subscription{
		ID:      id,
		Sink:    "http://localhost:8080",
		Filters: []*primitive.SubscriptionFilter{{Exact: map[string]string{"type": "test"}}},
	}
}

func makeEventRecord(t string) info.EventRecord {
	event := ce.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("source")
	event.SetType(t)
	return info.EventRecord{
		Event: &event,
	}
}

func TestChangeSubscription(t *testing.T) {
	Convey("test change subscription", t, func() {
		ctx := context.Background()
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		Convey("change target", func() {
			err := tg.Change(ctx, &primitive.Subscription{Sink: "test_sink"})
			So(err, ShouldBeNil)
		})
		Convey("change filter", func() {
			err := tg.Change(ctx, &primitive.Subscription{Filters: []*primitive.SubscriptionFilter{
				{Exact: map[string]string{"test": "test"}},
			}})
			So(err, ShouldBeNil)
		})
		Convey("change transformation", func() {
			err := tg.Change(ctx, &primitive.Subscription{Transformer: &primitive.Transformer{}})
			So(err, ShouldBeNil)
		})
		Convey("change config", func() {
			err := tg.Change(ctx, &primitive.Subscription{Config: primitive.SubscriptionConfig{RateLimit: 100}})
			So(err, ShouldBeNil)
		})
	})
}

func TestResetOffset(t *testing.T) {
	Convey("test reset offset", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ctx := context.Background()
		id := vanus.NewID()
		tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
		r := reader.NewMockReader(ctrl)
		tg.reader = r
		Convey("reset offset to timestamp", func() {
			offsets := pInfo.ListOffsetInfo{{EventLogID: vanus.NewID(), Offset: uint64(100)}}
			r.EXPECT().GetOffsetByTimestamp(gomock.Any(), gomock.Any()).Return(offsets, nil)
			v, err := tg.ResetOffsetToTimestamp(ctx, time.Now().Unix())
			So(err, ShouldBeNil)
			So(len(v), ShouldEqual, len(offsets))
		})
	})
}
