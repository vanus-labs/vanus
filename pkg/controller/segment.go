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
	// standard libraries.
	"context"
	"github.com/linkall-labs/vanus/observability/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"

	// third-party libraries.
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
)

var (
	_ io.Closer = (*segmentClient)(nil)
	_ Heartbeat = (*segmentClient)(nil)
)

type segmentClient struct {
	cc              *conn
	heartBeatClient ctrlpb.SegmentController_SegmentHeartbeatClient
}

func NewSegmentClient(ctrlAddrs []string, credentials credentials.TransportCredentials) ctrlpb.SegmentControllerClient {
	return &segmentClient{
		cc: newConn(ctrlAddrs, credentials),
	}
}

func (sc *segmentClient) Beat(ctx context.Context, v interface{}) error {
	log.Debug(ctx, "heartbeat", map[string]interface{}{
		"leader": sc.cc.leader,
	})
	req, ok := v.(*ctrlpb.SegmentHeartbeatRequest)
	if !ok {
		return ErrInvalidHeartBeatRequest
	}
	var err error
	makeSureClient := func() error {
		client := ctrlpb.NewSegmentControllerClient(sc.cc.makeSureClient(ctx, false))
		sc.heartBeatClient, err = client.SegmentHeartbeat(ctx)
		if err != nil {
			sts := status.Convert(err)
			if sts.Code() == codes.Unavailable {
				client = ctrlpb.NewSegmentControllerClient(sc.cc.makeSureClient(ctx, true))
				if client == nil {
					return ErrNoControllerLeader
				}
				sc.heartBeatClient, err = client.SegmentHeartbeat(ctx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		return nil
	}

	if sc.heartBeatClient == nil {
		if err = makeSureClient(); err != nil {
			return err
		}
	}

	if err := sc.heartBeatClient.Send(req); err != nil {
		if err = makeSureClient(); err != nil {
			return err
		}
		return err
	}

	return nil
}

func (sc *segmentClient) Close() error {
	if sc.heartBeatClient != nil {
		if err := sc.heartBeatClient.CloseSend(); err != nil {
			log.Warning(context.Background(), "close heartbeat stream error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}
	return sc.cc.close()
}

func (sc *segmentClient) RegisterSegmentServer(ctx context.Context, in *ctrlpb.RegisterSegmentServerRequest,
	opts ...grpc.CallOption) (*ctrlpb.RegisterSegmentServerResponse, error) {
	out := new(ctrlpb.RegisterSegmentServerResponse)
	err := sc.cc.invoke(ctx, "/linkall.vanus.controller.SegmentController/RegisterSegmentServer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) UnregisterSegmentServer(ctx context.Context, in *ctrlpb.UnregisterSegmentServerRequest, opts ...grpc.CallOption) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	out := new(ctrlpb.UnregisterSegmentServerResponse)
	err := sc.cc.invoke(ctx, "/linkall.vanus.controller.SegmentController/UnregisterSegmentServer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) ReportSegmentBlockIsFull(ctx context.Context,
	in *ctrlpb.SegmentHeartbeatRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := sc.cc.invoke(ctx, "/linkall.vanus.controller.SegmentController/ReportSegmentBlockIsFull", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) ReportSegmentLeader(ctx context.Context,
	in *ctrlpb.ReportSegmentLeaderRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := sc.cc.invoke(ctx, "/linkall.vanus.controller.SegmentController/ReportSegmentLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) QuerySegmentRouteInfo(ctx context.Context, in *ctrlpb.QuerySegmentRouteInfoRequest,
	opts ...grpc.CallOption) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
	out := new(ctrlpb.QuerySegmentRouteInfoResponse)
	err := sc.cc.invoke(ctx, "/linkall.vanus.controller.SegmentController/QuerySegmentRouteInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) SegmentHeartbeat(_ context.Context, _ ...grpc.CallOption) (ctrlpb.SegmentController_SegmentHeartbeatClient, error) {
	//TODO implement me
	panic("unsupported method, please use controller.RegisterHeartbeat")
}
