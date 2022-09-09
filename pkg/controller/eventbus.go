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

package controller

import (
	"context"
	"io"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ io.Closer = (*eventbusClient)(nil)
)

type eventbusClient struct {
	cc *conn
}

func NewEventbusClient(endpoints []string, credentials credentials.TransportCredentials) ctrlpb.EventBusControllerClient {
	return &eventbusClient{
		cc: newConn(endpoints, credentials),
	}
}

func (ec *eventbusClient) Close() error {
	return ec.cc.close()
}

func (ec *eventbusClient) CreateEventBus(ctx context.Context, in *ctrlpb.CreateEventBusRequest, opts ...grpc.CallOption) (*metapb.EventBus, error) {
	out := new(metapb.EventBus)
	err := ec.cc.invoke(ctx, "/linkall.vanus.controller.EventBusController/CreateEventBus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) DeleteEventBus(ctx context.Context, in *metapb.EventBus, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := ec.cc.invoke(ctx, "/linkall.vanus.controller.EventBusController/DeleteEventBus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) GetEventBus(ctx context.Context, in *metapb.EventBus, opts ...grpc.CallOption) (*metapb.EventBus, error) {
	out := new(metapb.EventBus)
	err := ec.cc.invoke(ctx, "/linkall.vanus.controller.EventBusController/GetEventBus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) ListEventBus(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ctrlpb.ListEventbusResponse, error) {
	out := new(ctrlpb.ListEventbusResponse)
	err := ec.cc.invoke(ctx, "/linkall.vanus.controller.EventBusController/ListEventBus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (ec *eventbusClient) UpdateEventBus(ctx context.Context, in *ctrlpb.UpdateEventBusRequest, opts ...grpc.CallOption) (*metapb.EventBus, error) {
	out := new(metapb.EventBus)
	err := ec.cc.invoke(ctx, "/linkall.vanus.controller.EventBusController/UpdateEventBus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
