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
	"fmt"
	"github.com/vanus-labs/vanus/observability/log"
	"net"
	"sync/atomic"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
)

func main() {
	var total int64
	ls, err := net.Listen("tcp4", ":18080")
	if err != nil {
		panic(err)
	}
	c, err := client.NewHTTP(cehttp.WithListener(ls), cehttp.WithRequestDataAtContextMiddleware())
	if err != nil {
		panic(err)
	}
	log.Info().Msg("start success")
	c.StartReceiver(context.Background(), func(e ce.Event) {
		fmt.Println(fmt.Sprintf("---total: %d", atomic.AddInt64(&total, 1)))
		fmt.Println(e)
	})
}
