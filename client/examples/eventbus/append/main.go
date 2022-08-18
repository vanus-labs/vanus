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
	"context"
	"log"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/vanus/client"
)

func main() {
	w, err := eb.OpenBusWriter("vanus://localhost:2048/eventbus/wwf123")
	if err != nil {
		log.Fatal(err)
	}

	// Create an Event.
	event := ce.NewEvent()
	event.SetID("example-event")
	event.SetSource("example/uri")
	event.SetType("example.type")
	event.SetData(ce.ApplicationJSON, map[string]string{"hello": "world"})

	eventID, err := w.Append(context.Background(), &event)
	if err != nil {
		log.Print(err.Error())
	} else {
		log.Printf("success! eventID:%s\n", eventID)
	}

	w.Close()
}
