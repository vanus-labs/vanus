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

package trigger_test

import (
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/trigger"
	"github.com/linkall-labs/vanus/observability/log"
	. "github.com/smartystreets/goconvey/convey"
	"net"
	"testing"
	"time"
)

func TestTrigger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go startSink(ctx)
	offsetManger := offset.NewOffsetManager()
	offsetManger.RegisterSubscription(1)
	tg := trigger.NewTrigger(nil, makeSubscription(1), offsetManger.GetSubscription(1))

	Convey("test", t, func() {
		_ = tg.EventArrived(ctx, makeEventRecord())
		_ = tg.Start()
		time.Sleep(time.Second * 1)
		So(tg.GetState(), ShouldEqual, trigger.TriggerRunning)
		tg.Stop()
		So(tg.GetState(), ShouldEqual, trigger.TriggerStopped)
		cancel()
	})

}

func startSink(ctx context.Context) {
	ls, err := net.Listen("tcp4", ":18080")
	if err != nil {
		panic(err)
	}
	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		panic(err)
	}
	_ = c.StartReceiver(ctx, func(e ce.Event) {
		log.Info(ctx, "receive event", map[string]interface{}{
			"event": e,
		})
	})
}

func makeSubscription(ID vanus.ID) *primitive.Subscription {
	return &primitive.Subscription{
		ID:      ID,
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
