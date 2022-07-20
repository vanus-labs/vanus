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
	"sync/atomic"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTrigger_ChangeTarget(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(&primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
	Convey("test change target", t, func() {
		So(tg.Target, ShouldEqual, "")
		So(tg.getCeClient(), ShouldBeNil)
		tg.ChangeTarget("http://localhost:18081")
		So(tg.getCeClient(), ShouldNotBeNil)
	})
}

func TestTrigger_ChangeFilter(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(&primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
	Convey("test change filter", t, func() {
		So(tg.getFilter(), ShouldBeNil)
		tg.ChangeFilter([]*primitive.SubscriptionFilter{{Exact: map[string]string{"type": "test"}}})
		So(tg.getFilter(), ShouldNotBeNil)
	})
}

func TestTrigger_ChangeInputTransformer(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(&primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
	Convey("test change input transformer", t, func() {
		So(tg.getInputTransformer(), ShouldBeNil)
		tg.ChangeInputTransformer(&primitive.InputTransformer{})
		So(tg.getInputTransformer(), ShouldBeNil)
		tg.ChangeInputTransformer(&primitive.InputTransformer{Define: map[string]string{"d": "d"}})
		So(tg.getInputTransformer(), ShouldNotBeNil)
		tg.ChangeInputTransformer(nil)
		So(tg.getInputTransformer(), ShouldBeNil)
	})
}

func TestTrigger_Options(t *testing.T) {
	Convey("test trigger option", t, func() {
		tg := &Trigger{}
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
		WithRetryPeriod(-1)(tg)
		So(tg.config.RetryPeriod, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithRetryPeriod(time.Duration(size))(tg)
		So(tg.config.RetryPeriod, ShouldEqual, size)
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
		WithRateLimit(-1)(tg)
		So(tg.config.RateLimit, ShouldEqual, 0)
		size = rand.Intn(1000) + size
		WithRateLimit(size)(tg)
		So(tg.config.RateLimit, ShouldEqual, size)
	})
}

func TestTriggerStartStop(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(makeSubscription(1), offsetManger.GetSubscription(1))
	Convey("test", t, func() {
		_ = tg.Start()
		time.Sleep(time.Second)
		So(tg.GetState(), ShouldEqual, TriggerRunning)
		tg.Stop()
		So(tg.GetState(), ShouldEqual, TriggerStopped)
	})
}

func TestTriggerRunEventSend(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(makeSubscription(1), offsetManger.GetSubscription(1))
	tg.ceClient = NewFakeClient("test")
	ctx := context.Background()
	Convey("test event run process", t, func() {
		size := 10
		for i := 0; i < size; i++ {
			_ = tg.EventArrived(ctx, makeEventRecord("test"))
		}
		So(len(tg.eventCh), ShouldEqual, size)
		go func() {
			tg.runEventProcess(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
		So(len(tg.sendCh), ShouldEqual, size)
		_ = tg.EventArrived(ctx, makeEventRecord("no"))
		time.Sleep(100 * time.Millisecond)
		So(len(tg.sendCh), ShouldEqual, size)
		close(tg.eventCh)
		go func() {
			tg.runEventSend(ctx)
		}()
		time.Sleep(100 * time.Millisecond)
		close(tg.sendCh)
	})
}

func TestTriggerRateLimit(t *testing.T) {
	Convey("test rate limit", t, func() {
		offsetManger := offset.NewOffsetManager()
		offsetManger.RegisterSubscription(1)
		tg := NewTrigger(makeSubscription(1), offsetManger.GetSubscription(1))
		tg.ceClient = NewFakeClient("test")
		ctx := context.Background()
		rateLimit := 10000
		Convey("test no rate limit", func() {
			c := testSendEvent(ctx, tg)
			So(c, ShouldBeGreaterThan, rateLimit)
		})
		Convey("test with rate", func() {
			WithRateLimit(rateLimit)(tg)
			c := testSendEvent(ctx, tg)
			So(c, ShouldBeLessThanOrEqualTo, 2*rateLimit+1)
		})
	})
}

func testSendEvent(ctx context.Context, tg *Trigger) int64 {
	size := 50000
	eventCh := make(chan *ce.Event, size)
	for i := 0; i < size; i++ {
		eventCh <- makeEventRecord("test").Event
	}
	var c int64
	go func() {
		for event := range eventCh {
			tg.retrySendEvent(ctx, event)
			atomic.AddInt64(&c, 1)
		}
	}()
	time.Sleep(2 * time.Second)
	close(eventCh)
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
		EventOffset: info.EventOffset{
			Event: &event,
		},
	}
}
