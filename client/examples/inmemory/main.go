// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	// standard libraries.
	"context"
	"fmt"
	"io"
	"log"

	// third-party libraries.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	eb "github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/discovery"
	"github.com/linkall-labs/vanus/client/pkg/discovery/record"
	"github.com/linkall-labs/vanus/client/pkg/inmemory"
)

var (
	// TODO: format of vrn
	ebVRN = "vanus+local:///eventbus/1"
	elVRN = "vanus+inmemory:///eventlog/1?eventbus=1&keepalive=true"
	br    = &record.EventBus{
		VRN: ebVRN,
		Logs: []*record.EventLog{
			{
				VRN:  elVRN,
				Mode: record.PremWrite | record.PremRead,
			},
		},
	}
)

func init() {
	inmemory.UseInMemoryLog("vanus+inmemory")
	ns := inmemory.UseNameService("vanus+local")
	// register metadata of eventbus
	vrn, err := discovery.ParseVRN(ebVRN)
	if err != nil {
		panic(err.Error())
	}
	ns.Register(vrn, br)
}

func doAppend(ctx context.Context) {
	w, err := eb.OpenBusWriter(ctx, ebVRN)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		// Create an Event.
		event := ce.NewEvent()
		event.SetID(fmt.Sprintf("%d", i))
		event.SetSource("example/uri")
		event.SetType("example.type")
		event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

		_, err = w.Append(ctx, &event)
		if err != nil {
			log.Print(err)
		}
	}

	w.Close(ctx)
}

func doRead(ctx context.Context) {
	ls, err := eb.LookupReadableLogs(ctx, ebVRN)
	if err != nil {
		log.Fatal(err)
	}

	r, err := eb.OpenLogReader(ctx, ls[0].VRN, eb.DisablePolling())
	if err != nil {
		log.Fatal(err)
	}

	_, err = r.Seek(ctx, 3, io.SeekCurrent)
	if err != nil {
		log.Fatal(err)
	}

	for {
		events, err := r.Read(ctx, 5)
		if err != nil {
			log.Fatal(err)
		}

		if len(events) == 0 {
			break
		}

		for _, e := range events {
			log.Printf("event: %v\n", e)
		}
	}

	r.Close(ctx)
}

func main() {
	ctx := context.Background()
	doAppend(ctx)
	doRead(ctx)
}
