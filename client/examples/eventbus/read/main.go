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

	// this project.
	"github.com/linkall-labs/vanus/client"
	"github.com/linkall-labs/vanus/client/pkg/option"
	"github.com/linkall-labs/vanus/client/pkg/policy"
)

func main() {
	ctx := context.Background()

	c := client.Connect([]string{"localhost:2048"})
	eb := c.Eventbus(ctx, "quick-start")
	ls, err := eb.ListLog(ctx)
	if err != nil {
		log.Print(err.Error())
	}
	r := eb.Reader(option.WithReadPolicy(policy.NewManuallyReadPolicy(ls[0], 0)))
	events, offset, eventlogID, err := r.Read(ctx)
	if err != nil {
		log.Print(err.Error())
	} else {
		log.Println("success!")
		log.Printf("events: %+v\n", events)
		log.Printf("offset: %d\n", offset)
		log.Printf("eventlog id: %d\n", eventlogID)
	}
}
