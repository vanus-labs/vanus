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
	"time"

	"github.com/linkall-labs/vanus/client/pkg/codec"

	"github.com/linkall-labs/vanus/observability/log"
	"google.golang.org/grpc/credentials/insecure"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	stdGrpc "google.golang.org/grpc"
)

type grpc struct {
	client cloudevents.CloudEventsClient
}

func NewGRPCClient(url string) EventClient {
	opts := []stdGrpc.DialOption{
		stdGrpc.WithBlock(),
		stdGrpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := stdGrpc.DialContext(ctx, url, opts...)
	if err != nil {
		log.Error(context.TODO(), "failed to connector url", map[string]interface{}{
			log.KeyError: err,
			"url":        url,
		})
		return nil
	}
	c := cloudevents.NewCloudEventsClient(conn)
	return &grpc{
		client: c,
	}
}

func (c *grpc) Send(ctx context.Context, events ...*ce.Event) Result {
	es := make([]*cloudevents.CloudEvent, len(events))
	for idx := range events {
		es[idx], _ = codec.ToProto(events[idx])
	}
	c.client.Send(ctx, &cloudevents.BatchEvent{
		Events: &cloudevents.CloudEventBatch{Events: es},
	})
	return Success
}
