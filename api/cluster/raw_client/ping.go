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

	ctrlpb "github.com/vanus-labs/vanus/api/controller"
)

var _ ctrlpb.PingServerClient = (*pingClient)(nil)

func NewPingClient(cc *Conn) ctrlpb.PingServerClient {
	return &pingClient{
		cc: cc,
	}
}

type pingClient struct {
	cc *Conn
}

func (p pingClient) Ping(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ctrlpb.PingResponse, error) {
	out := new(ctrlpb.PingResponse)
	err := p.cc.invoke(ctx, "/vanus.core.controller.PingServer/Ping", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
