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
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/linkall-labs/vanus/observability/log"
	"net"
)

func main() {
	ls, err := net.Listen("tcp4", ":18080")
	if err != nil {
		panic(err)
	}
	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		panic(err)
	}
	log.Info(context.Background(), "start success", nil)
	c.StartReceiver(context.Background(), func(e ce.Event) {
		log.Info(context.Background(), "receive event", map[string]interface{}{
			"event": e,
		})
	})
}
