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

package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller"
	"github.com/linkall-labs/vanus/internal/primitive"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	triggerWorkerIP   = "0.0.0.0"
	triggerWorkerPort = 2049
	SinkURI           = url.URL{Scheme: "http", Host: "localhost:8080"}
)

func main() {
	m := controller.NewTriggerController()
	m.Start()
	m.AddTriggerProcessor(fmt.Sprintf("%s:%d", triggerWorkerIP, triggerWorkerPort))
	// for test
	go func() {
		for {
			sub := &primitive.Subscription{
				ID:      uuid.NewString(),
				Sink:    primitive.URI(SinkURI.String()),
				Filters: []primitive.SubscriptionFilter{{Exact: map[string]string{"type": "test"}}},
			}
			m.AddSubscription(sub)
			time.Sleep(10 * time.Minute)
		}
	}()

	exitCh := make(chan os.Signal)
	signal.Notify(exitCh, os.Interrupt, syscall.SIGTERM)
	<-exitCh
	m.Stop()
}
