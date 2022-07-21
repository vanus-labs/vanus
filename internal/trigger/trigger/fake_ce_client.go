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

package trigger

import (
	"context"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/linkall-labs/vanus/internal/primitive"
)

type fakeClient struct {
}

func (c *fakeClient) Send(ctx context.Context, out event.Event) protocol.Result {
	return ce.ResultACK
}

func (c *fakeClient) Request(ctx context.Context, out event.Event) (*event.Event, protocol.Result) {
	return nil, ce.ResultACK
}

func (c *fakeClient) StartReceiver(ctx context.Context, fn interface{}) error {
	return nil
}

func NewFakeClient(target primitive.URI) ce.Client {
	return &fakeClient{}
}
