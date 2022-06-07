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

package errinterceptor

import (
	"context"
	"fmt"

	"github.com/linkall-labs/vanus/proto/pkg/errors"
	"google.golang.org/grpc"
)

type GRPCErrorTranslatorFunc func(*errors.Error) error

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo,
		handler grpc.StreamHandler) error {
		err := handler(srv, stream)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return err
	}
}

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		res, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return res, err
	}
}

// convertToGRPCError convert an internal error to an exported error defined in gRPC.
func convertToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	grpcErr := &errors.Error{}
	e, ok := err.(*errors.ErrorType)
	if ok {
		grpcErr = e.GRPCError()
	} else {
		grpcErr.Code = errors.ErrorCode_UNKNOWN
		grpcErr.Message = err.Error()
	}

	return fmt.Errorf("{\"code\":\"%v\",\"message\":\"%s\"}",
		grpcErr.Code, grpcErr.Message)
}
