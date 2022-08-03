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

package reader_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/inmemory"
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/prashantv/gostub"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReader(t *testing.T) {
	gostub.Stub(&eb.LookupLatestLogOffset, func(ctx context.Context, vrn string) (int64, error) {
		return 0, nil
	})
	testSendInmemory()
	// memoryEbVRN := "vanus+local:eventbus:1".
	memoryEbVRN := "vanus+local:///eventbus/1"
	conf := reader.Config{
		EventBusName:   "testBus",
		EventBusVRN:    memoryEbVRN,
		SubscriptionID: 1,
	}
	events := make(chan info.EventRecord, 10)
	r := reader.NewReader(conf, events)
	r.Start()
	var testC, noneC int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range events {
			if e.Event.Type() == "test" {
				testC++
			} else {
				noneC++
			}
		}
	}()
	// wait read complete .
	time.Sleep(time.Second)
	r.Close()
	close(events)
	wg.Wait()
	Convey("test reader", t, func() {
		So(len(events), ShouldEqual, 0)
		So(testC, ShouldEqual, 50)
		So(noneC, ShouldEqual, 50)
	})
}

func testSendInmemory() {
	ebVRN := "vanus+local:///eventbus/1"
	elVRN := "vanus+inmemory:///eventlog/1?eventbus=1&keepalive=true"
	br := &record.EventBus{
		VRN: ebVRN,
		Logs: []*record.EventLog{
			{
				VRN:  elVRN,
				Mode: record.PremWrite | record.PremRead,
			},
		},
	}

	inmemory.UseInMemoryLog("vanus+inmemory")
	ns := inmemory.UseNameService("vanus+local")
	// register metadata of eventbus
	vrn, err := discovery.ParseVRN(ebVRN)
	if err != nil {
		panic(err.Error())
	}
	ns.Register(vrn, br)
	bw, err := eb.OpenBusWriter(ebVRN)
	if err != nil {
		log.Error(context.Background(), "open bus writer error", map[string]interface{}{"error": err})
		os.Exit(1)
	}

	go func() {
		i := 1
		for ; i <= 100; i++ {
			tp := "test"
			if i%2 == 0 {
				// time.Sleep(1 * time.Second).
				tp = "none"
			}
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType(tp)
			event.SetExtension("vanus", fmt.Sprintf("value%d", i))
			_ = event.SetData(ce.ApplicationJSON, map[string]string{"hello": fmt.Sprintf("world %d", i), "type": tp})

			_, err = bw.Append(context.Background(), &event)
			if err != nil {
				log.Error(context.Background(), "append event error", map[string]interface{}{"error": err})
			}
		}
	}()
}
