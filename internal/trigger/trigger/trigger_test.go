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
	. "github.com/smartystreets/goconvey/convey"

	eb "github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"

	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/internal/trigger/client"
	"github.com/vanus-labs/vanus/internal/trigger/info"
	"github.com/vanus-labs/vanus/internal/trigger/reader"
)

func TestTrigger_Options(t *testing.T) {
	Convey("test trigger option", t, func() {
		tg := &trigger{}
		size := rand.Intn(1000) + 1
		WithDeliveryTimeout(0)(tg)
		So(tg.config.DeliveryTimeout, ShouldEqual, defaultDeliveryTimeout)
		size = rand.Intn(1000) + size
		WithDeliveryTimeout(uint32(size))(tg)
		So(tg.config.DeliveryTimeout, ShouldEqual, time.Duration(size)*time.Millisecond)
		WithMaxRetryAttempts(-1)(tg)
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
		WithRateLimit(uint32(size))(tg)
		So(tg.config.RateLimit, ShouldEqual, size)
	})
}

func TestTriggerStartStop(t *testing.T) {
	Convey("test start and stop", t, func() {
		id := vanus.NewTestID()
		tg, err := newTrigger(makeSubscription(id), WithControllers([]string{"test"}))
		So(err, ShouldBeNil)
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r := reader.NewMockReader(ctrl)
		r2 := reader.NewMockReader(ctrl)
		ctx := context.Background()
		mockClient := eb.NewMockClient(ctrl)
		mockEventbus := api.NewMockEventbus(ctrl)
		mockBusWriter := api.NewMockBusWriter(ctrl)
		mockBusReader := api.NewMockBusReader(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		tg.client = mockClient
		err = tg.Init(ctx)
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
		id := vanus.NewTestID()
		tg, err := newTrigger(makeSubscription(id), WithControllers([]string{"test"}))
		So(err, ShouldBeNil)
		mockClient := eb.NewMockClient(ctrl)
		mockEventbus := api.NewMockEventbus(ctrl)
		mockBusWriter := api.NewMockBusWriter(ctrl)
		mockBusReader := api.NewMockBusReader(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		tg.client = mockClient
		_ = tg.Init(ctx)
		e := makeEventRecord("type")
		tg.dlEventWriter = mockBusWriter
		tg.timerEventWriter = mockBusWriter
		var callCount int
		mockBusWriter.EXPECT().Append(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(context.Context,
			*cloudevents.CloudEventBatch, ...api.WriteOption,
		) ([]string, error) {
			callCount++
			if callCount%2 != 0 {
				return []string{""}, fmt.Errorf("append error")
			}
			return []string{""}, nil
		})
		Convey("test no need retry,in dlq", func() {
			tg.writeFailEvent(ctx, e.Event, 400, fmt.Errorf("400 error"))
			So(e.Event.Extensions()[primitive.DeadLetterReason], ShouldNotBeNil)
		})
		Convey("test first retry,in retry", func() {
			tg.writeFailEvent(ctx, e.Event, 500, fmt.Errorf("500 error"))
			So(e.Event.Extensions()[primitive.XVanusRetryAttempts], ShouldEqual, 1)
			So(e.Event.Extensions()[primitive.XVanusSubscriptionID], ShouldEqual, id.String())
		})
		Convey("test retry again,in retry", func() {
			attempts := 1
			e.Event.SetExtension(primitive.XVanusRetryAttempts, strconv.Itoa(attempts))
			tg.writeFailEvent(ctx, e.Event, 500, fmt.Errorf("500 error"))
			So(e.Event.Extensions()[primitive.XVanusRetryAttempts], ShouldEqual, attempts+1)
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
		id := vanus.NewTestID()
		tg, err := newTrigger(makeSubscription(id), WithControllers([]string{"test"}))
		So(err, ShouldBeNil)
		mockClient := eb.NewMockClient(ctrl)
		mockEventbus := api.NewMockEventbus(ctrl)
		mockBusWriter := api.NewMockBusWriter(ctrl)
		mockBusReader := api.NewMockBusReader(ctrl)
		mockClient.EXPECT().Eventbus(gomock.Any(), gomock.Any()).AnyTimes().Return(mockEventbus)
		mockEventbus.EXPECT().Writer().AnyTimes().Return(mockBusWriter)
		mockEventbus.EXPECT().Reader().AnyTimes().Return(mockBusReader)
		tg.client = mockClient
		_ = tg.Init(ctx)
		tg.eventCli = cli
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
			tg.runEventFilterTransform(ctx)
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
			tg.runEventToBatch(ctx)
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
		id := vanus.NewTestID()
		tg, err := newTrigger(makeSubscription(id), WithControllers([]string{"test"}))
		So(err, ShouldBeNil)
		tg.eventCli = cli
		cli.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().Return(client.Success)
		rateLimit := uint32(10000)
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
				_ = tg.sendEvent(ctx, event)
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
		ID:   id,
		Sink: "http://localhost:8080",
		Filters: []*primitive.SubscriptionFilter{{
			Exact: map[string]string{"type": "test"},
		}},
		EventbusID:           vanus.NewTestID(),
		TimerEventbusID:      vanus.NewTestID(),
		DeadLetterEventbusID: vanus.NewTestID(),
		RetryEventbusID:      vanus.NewTestID(),
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
		id := vanus.NewTestID()
		tg, err := newTrigger(makeSubscription(id), WithControllers([]string{"test"}))
		So(err, ShouldBeNil)
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
			err := tg.Change(ctx, &primitive.Subscription{
				Config: primitive.SubscriptionConfig{RateLimit: 100},
			})
			So(err, ShouldBeNil)
		})
	})
}
