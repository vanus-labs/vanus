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

package worker

import (
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/linkall-labs/eventbus-go/pkg/discovery"
	"github.com/linkall-labs/eventbus-go/pkg/discovery/record"
	"github.com/linkall-labs/eventbus-go/pkg/inmemory"
	"github.com/linkall-labs/vanus/observability/log"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestEventBusWrite(t *testing.T) {
	ebVRN := "vanus+local:eventbus:example"
	elVRN := "vanus+local:eventlog+inmemory:1?keepalive=true"
	elVRN2 := "vanus+local:eventlog+inmemory:2?keepalive=true"
	br := &record.EventBus{
		VRN: ebVRN,
		Logs: []*record.EventLog{
			{
				VRN:  elVRN,
				Mode: record.PremWrite | record.PremRead,
			},
			{
				VRN:  elVRN2,
				Mode: record.PremWrite | record.PremRead,
			},
		},
	}

	inmemory.UseInMemoryLog("inmemory")
	ns := inmemory.UseNameService("vanus+local")
	// register metadata of eventbus
	vrn, err := discovery.ParseVRN(ebVRN)
	if err != nil {
		panic(err.Error())
	}
	ns.Register(vrn, br)
	bw, err := eb.OpenBusWriter(ebVRN)
	if err != nil {
		log.Fatal("open bus writer error", map[string]interface{}{"error": err})
	}

	go func() {
		i := 1
		tp := "none"
		for ; i < 10000; i++ {
			if i%3 == 0 {
				tp = "test"
			}
			if i%2 == 0 {
				time.Sleep(10 * time.Second)
				tp = "none"
			}
			// Create an Event.
			event := ce.NewEvent()
			event.SetID(fmt.Sprintf("%d", i))
			event.SetSource("example/uri")
			event.SetType(tp)
			event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world", "type": tp})

			_, err = bw.Append(&event)
			if err != nil {
				log.Error("append event error", map[string]interface{}{"error": err})
			}
		}
	}()
	go http.ListenAndServe(":18080", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			log.Error("receive error", map[string]interface{}{"error": err})
		} else {
			log.Info("sink receive event", map[string]interface{}{
				"event": string(body),
			})
		}
	}))
	time.Sleep(time.Hour)
}
