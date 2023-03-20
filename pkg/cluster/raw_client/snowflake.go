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

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
)

func NewSnowflakeController(cc *Conn) ctrlpb.SnowflakeControllerClient {
	return &snowflakeClient{
		cc: cc,
	}
}

type snowflakeClient struct {
	cc *Conn
}

func (sfc *snowflakeClient) Close() error {
	return sfc.cc.close()
}

func (sfc *snowflakeClient) GetClusterStartTime(
	ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption,
) (*timestamppb.Timestamp, error) {
	out := &timestamppb.Timestamp{}
	err := sfc.cc.invoke(ctx, "/vanus.core.controller.SnowflakeController/GetClusterStartTime", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sfc *snowflakeClient) RegisterNode(
	ctx context.Context, in *wrapperspb.UInt32Value, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := &emptypb.Empty{}
	err := sfc.cc.invoke(ctx, "/vanus.core.controller.SnowflakeController/RegisterNode", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sfc *snowflakeClient) UnregisterNode(
	ctx context.Context, in *wrapperspb.UInt32Value, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := &emptypb.Empty{}
	err := sfc.cc.invoke(ctx, "/vanus.core.controller.SnowflakeController/UnregisterNode", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
