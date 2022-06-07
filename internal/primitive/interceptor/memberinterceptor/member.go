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

package memberinterceptor

import (
	"context"
	"fmt"

	embedetcd "github.com/linkall-labs/embed-etcd"
	rpcerr "github.com/linkall-labs/vanus/proto/pkg/errors"
	"google.golang.org/grpc"
)

func StreamServerInterceptor(member embedetcd.Member) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !member.IsLeader() {
			// TODO  read-only request bypass
			return rpcerr.New(fmt.Sprintf("i'm not leader, please connect to: %s",
				member.GetLeaderAddr())).WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
		}
		return handler(srv, stream)
	}
}

func UnaryServerInterceptor(member embedetcd.Member) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		if info.FullMethod != "/linkall.vanus.controller.PingServer/Ping" &&
			!member.IsLeader() {
			// TODO  read-only request bypass
			return nil, rpcerr.New(fmt.Sprintf("i'm not leader, please connect to: %s",
				member.GetLeaderAddr())).WithGRPCCode(rpcerr.ErrorCode_NOT_LEADER)
		}
		return handler(ctx, req)
	}
}
