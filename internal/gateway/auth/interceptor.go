// Copyright 2023 Linkall Inc.
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

package auth

import (
	"context"

	"google.golang.org/grpc"
)

type AuthorizationFunc func(ctx context.Context, method string, req interface{}) (err error)

func UnaryServerInterceptor(authFunc AuthorizationFunc) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{},
		info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		err := authFunc(ctx, info.FullMethod, req)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	// todo stream authorize
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, stream)
	}
}
