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

package client

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/linkall-labs/vanus/client/pkg/codec"

	"google.golang.org/grpc/credentials/insecure"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	stdGrpc "google.golang.org/grpc"
)

type grpc struct {
	client cloudevents.CloudEventsClient
	url    string
	lock   sync.Mutex
}

func NewGRPCClient(url string) EventClient {
	return &grpc{
		url: url,
	}
}

func (c *grpc) init() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if c.client != nil {
		return nil
	}
	opts := []stdGrpc.DialOption{
		stdGrpc.WithBlock(),
		stdGrpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	//nolint:gomnd //wrong check
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := stdGrpc.DialContext(ctx, c.url, opts...)
	if err != nil {
		return err
	}
	c.client = cloudevents.NewCloudEventsClient(conn)
	return nil
}

func (c *grpc) Send(ctx context.Context, events ...*ce.Event) Result {
	if c.client == nil {
		err := c.init()
		if err != nil {
			return newUndefinedErr(err)
		}
	}
	es := make([]*cloudevents.CloudEvent, len(events))
	for idx := range events {
		es[idx], _ = codec.ToProto(events[idx])
	}
	_, err := c.client.Send(ctx, &cloudevents.BatchEvent{
		Events: &cloudevents.CloudEventBatch{Events: es},
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return DeliveryTimeout
		}
		return newUndefinedErr(err)
	}
	return Success
}
