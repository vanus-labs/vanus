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
	"log"

	// third-party project.
	ce "github.com/cloudevents/sdk-go/v2"

	// this project.
	"github.com/vanus-labs/vanus/client"
	"github.com/vanus-labs/vanus/client/pkg/api"
	"github.com/vanus-labs/vanus/client/pkg/option"
	"github.com/vanus-labs/vanus/client/pkg/policy"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	"github.com/vanus-labs/vanus/proto/pkg/codec"
)

func main() {
	ctx := context.Background()

	c := client.Connect([]string{"localhost:2048"})

	eventbusID, err := vanus.NewIDFromString("0000002689000012")
	if err != nil {
		panic("invalid id")
	}
	bus, err := c.Eventbus(ctx, api.WithID(eventbusID.Uint64()))
	if err != nil {
		panic(err.Error())
	}
	w := bus.Writer()
	// Create an Event.
	event := ce.NewEvent()
	event.SetID("example-event")
	event.SetSource("example/uri")
	event.SetType("example.type")
	event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

	eventpb, err := codec.ToProto(&event)
	if err != nil {
		return
	}
	ceBatch := &cloudevents.CloudEventBatch{
		Events: []*cloudevents.CloudEvent{eventpb},
	}
	eventID, err := w.Append(ctx, ceBatch, option.WithWritePolicy(policy.NewRoundRobinWritePolicy(bus)))
	if err != nil {
		log.Print(err.Error())
	} else {
		log.Printf("success! eventID:%s\n", eventID)
	}
}
