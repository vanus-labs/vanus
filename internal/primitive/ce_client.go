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

package primitive

import (
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/cloudevents/sdk-go/v2/protocol/http"
)

type client struct {
	ceClient ce.Client
}

func (c *client) Send(ctx context.Context, out event.Event) protocol.Result {
	res := c.ceClient.Send(ctx, out)
	return res
}

func (c *client) Request(ctx context.Context, out event.Event) (*event.Event, protocol.Result) {
	resp, res := c.ceClient.Request(ctx, out)
	return resp, res
}

func (c *client) StartReceiver(ctx context.Context, fn interface{}) error {
	return c.ceClient.StartReceiver(ctx, fn)
}

func NewCeClient(target URI) (ce.Client, error) {
	opts := make([]http.Option, 0)
	if target != "" {
		opts = append(opts, ce.WithTarget(string(target)))
	}
	ceClient, err := ce.NewClientHTTP(opts...)
	if err != nil {
		return nil, err
	}
	return &client{ceClient: ceClient}, nil
}
