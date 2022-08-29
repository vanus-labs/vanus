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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	pInfo "github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/client"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTrigger_Options(t *testing.T) {
	Convey("test trigger option", t, func() {
		tg := &trigger{}
		WithFilterProcessSize(-1)(tg)
		So(tg.config.FilterProcessSize, ShouldEqual, 0)
		size := rand.Intn(1000) + 1
		WithFilterProcessSize(size)(tg)
		So(tg.config.FilterProcessSize, ShouldEqual, size)
		WithSendProcessSize(-1)(tg)
		So(tg.config.SendProcessSize, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithSendProcessSize(size)(tg)
		So(tg.config.SendProcessSize, ShouldEqual, size)
		WithSendTimeOut(-1)(tg)
		So(tg.config.SendTimeOut, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithSendTimeOut(time.Duration(size))(tg)
		So(tg.config.SendTimeOut, ShouldEqual, size)
		WithRetryInterval(-1)(tg)
		So(tg.config.RetryInterval, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithRetryInterval(time.Duration(size))(tg)
		So(tg.config.RetryInterval, ShouldEqual, size)
		WithMaxRetryTimes(-1)(tg)
		So(tg.config.MaxRetryTimes, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithMaxRetryTimes(size)(tg)
		So(tg.config.MaxRetryTimes, ShouldEqual, size)
		WithBufferSize(-1)(tg)
		So(tg.config.BufferSize, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithBufferSize(size)(tg)
		So(tg.config.BufferSize, ShouldEqual, size)
		WithRateLimit(0)(tg)
		So(tg.config.RateLimit, ShouldEqual, 0)
		WithRateLimit(-1)(tg)
		So(tg.config.RateLimit, ShouldEqual, -1)
		size = rand.Intn(1000) + size
		WithRateLimit(int32(size))(tg)
		So(tg.config.RateLimit, ShouldEqual, size)
	})
}

func TestTriggerStartStop(t *testing.T) {
	id := vanus.NewID()
	tg := NewTrigger(makeSubscription(id), WithControllers([]string{"test"})).(*trigger)
	Convey("test start and stop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		r := reader.NewMockReader(ctrl)
		ctx := context.Background()
		err := tg.Init(ctx)
		So(err, ShouldBeNil)
		tg.reader = r
		r.EXPECT().Start().Return(nil)
		err = tg.Start(ctx)
		So(err, ShouldBeNil)
		time.Sleep(100 * time.Millisecond)
		So(tg.state, ShouldEqual, TriggerRunning)
		r.EXPECT().Close().Return()
		_ = tg.Stop(ctx)
		So(tg.state, ShouldEqual, TriggerStopped)
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
		_ = tg.Init(ctx)
		tg.client = cli
		cli.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		size := 10
		for i := 0; i < size; i++ {
			_ = tg.eventArrived(ctx, makeEventRecord("test"))
		}
		So(len(tg.eventCh), ShouldEqual, size)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			tg.runEventProcess(ctx)
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
		cli.EXPECT().Send(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
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
				_ = tg.retrySendEvent(ctx, event)
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
