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

package interceptor

import (
	"context"
	"encoding/hex"
	"github.com/golang/protobuf/proto"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vsproto/pkg/errors"
	"google.golang.org/grpc"
)

type GRPCErrorTranslatorFunc func(*errors.Error) error

func GRPCErrorClientInboundInterceptor(f GRPCErrorTranslatorFunc) []grpc.DialOption {
	opts := make([]grpc.DialOption, 0)
	opts = append(opts, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		return f(getGRPCError(err))
	}))
	// TODO add stream interceptor
	return opts
}

func GRPCErrorServerOutboundInterceptor() []grpc.ServerOption {
	opts := make([]grpc.ServerOption, 0)
	opts = append(opts, grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		res, err := handler(ctx, req)
		if err != nil {
			err = convertToGRPCError(err)
		}
		return res, err
	}))
	// TODO add stream interceptor
	return opts
}

// convertToGRPCError convert an internal error to an exported error defined in gRPC
func convertToGRPCError(err error) error {
	if err != nil {
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

	data, _ := proto.Marshal(grpcErr)
	return errors.New(hex.EncodeToString(data))
}

func getGRPCError(err error) *errors.Error {
	if err == nil {
		return nil
	}

	data, err := hex.DecodeString(err.Error())
	if err != nil {
		msg := "invalid hex format, received a unrecognized error"
		log.Warning(nil, msg, map[string]interface{}{
			log.KeyError: err,
		})
		return &errors.Error{
			Message: msg,
			Code:    0,
		}
	}

	grpcErr := &errors.Error{}
	err = proto.Unmarshal(data, grpcErr)
	if err != nil {
		msg := "invalid hex format, received a unrecognized error"
		log.Warning(nil, msg, map[string]interface{}{
			log.KeyError: err,
		})
		return &errors.Error{
			Message: msg,
			Code:    0,
		}
	}

	return grpcErr
}
