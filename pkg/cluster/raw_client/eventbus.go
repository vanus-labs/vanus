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

package raw_client

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

var _ io.Closer = (*eventbusClient)(nil)

type eventbusClient struct {
	cc *Conn
}

func NewEventbusClient(cc *Conn) ctrlpb.EventbusControllerClient {
	return &eventbusClient{
		cc: cc,
	}
}

func (ec *eventbusClient) Close() error {
	return ec.cc.close()
}

func (ec *eventbusClient) CreateEventbus(
	ctx context.Context, in *ctrlpb.CreateEventbusRequest, opts ...grpc.CallOption,
) (*metapb.Eventbus, error) {
	out := new(metapb.Eventbus)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/CreateEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) CreateSystemEventbus(
	ctx context.Context, in *ctrlpb.CreateEventbusRequest, opts ...grpc.CallOption,
) (*metapb.Eventbus, error) {
	out := new(metapb.Eventbus)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/CreateSystemEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) DeleteEventbus(
	ctx context.Context, in *ctrlpb.DeleteEventbusRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/DeleteEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) GetEventbus(
	ctx context.Context, in *ctrlpb.GetEventbusRequest, opts ...grpc.CallOption,
) (*metapb.Eventbus, error) {
	out := new(metapb.Eventbus)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/GetEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) ListEventbus(
	ctx context.Context, in *ctrlpb.ListEventbusRequest, opts ...grpc.CallOption,
) (*ctrlpb.ListEventbusResponse, error) {
	out := new(ctrlpb.ListEventbusResponse)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/ListEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) UpdateEventbus(
	ctx context.Context, in *ctrlpb.UpdateEventbusRequest, opts ...grpc.CallOption,
) (*metapb.Eventbus, error) {
	out := new(metapb.Eventbus)
	err := ec.cc.invoke(ctx, "/vanus.core.controller.EventbusController/UpdateEventbus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
