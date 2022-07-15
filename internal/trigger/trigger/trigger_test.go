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
	"sync"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
	ceClient "github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTrigger_ChangeTarget(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(nil, &primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
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
	tg := NewTrigger(nil, &primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
	Convey("test change filter", t, func() {
		So(tg.getFilter(), ShouldBeNil)
		tg.ChangeFilter([]*primitive.SubscriptionFilter{{Exact: map[string]string{"type": "test"}}})
		So(tg.getFilter(), ShouldNotBeNil)
	})
}

func TestTrigger_ChangeInputTransformer(t *testing.T) {
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(nil, &primitive.Subscription{ID: 1}, offsetManger.GetSubscription(1))
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

func TestTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go startSink(ctx, &wg)
	time.Sleep(time.Second)
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := NewTrigger(&Config{SendTimeOut: time.Millisecond * 100, RetryPeriod: time.Millisecond * 100}, makeSubscription(1), offsetManger.GetSubscription(1))

	Convey("test", t, func() {
		wg.Add(1)
		_ = tg.EventArrived(ctx, makeEventRecord())
		_ = tg.Start()
		wg.Wait()
		time.Sleep(time.Second)
		So(tg.GetState(), ShouldEqual, TriggerRunning)
		tg.Stop()
		So(tg.GetState(), ShouldEqual, TriggerStopped)
		cancel()
	})
}

func startSink(ctx context.Context, wg *sync.WaitGroup) {
	c, err := ceClient.NewHTTP(cehttp.WithPort(18080))
	if err != nil {
		panic(err)
	}
	_ = c.StartReceiver(ctx, func(e ce.Event) {
		defer wg.Done()
		log.Info(ctx, "receive event", map[string]interface{}{
			"event": e,
		})
	})
}

func makeSubscription(id vanus.ID) *primitive.Subscription {
	return &primitive.Subscription{
		ID:      id,
		Sink:    "http://localhost:18080",
		Filters: []*primitive.SubscriptionFilter{{Exact: map[string]string{"type": "type"}}},
	}
}

func makeEventRecord() info.EventRecord {
	event := ce.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("source")
	event.SetType("type")
	return info.EventRecord{
		EventOffset: info.EventOffset{
			Event: &event,
		},
	}
}
