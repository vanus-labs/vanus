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

	"google.golang.org/grpc"

	"github.com/vanus-labs/vanus/pkg/errors"

	"github.com/linkall-labs/vanus/internal/controller/member"
)

func StreamServerInterceptor(mem member.Member) grpc.StreamServerInterceptor {
	return func(
		srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		if !mem.IsLeader() {
			// TODO  read-only request bypass
			return errors.ErrNotLeader.WithMessage(
				fmt.Sprintf("i'm not leader, please connect to: %s", mem.GetLeaderAddr()))
		}
		return handler(srv, stream)
	}
}

func UnaryServerInterceptor(mem member.Member) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		if info.FullMethod != "/linkall.vanus.controller.PingServer/Ping" &&
			!mem.IsLeader() {
			// TODO  read-only request bypass
			return nil, errors.ErrNotLeader.WithMessage(
				fmt.Sprintf("i'm not leader, please connect to: %s", mem.GetLeaderAddr()))
		}
		return handler(ctx, req)
	}
}
