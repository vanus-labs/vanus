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
	// standard libraries.
	"context"
	"io"

	// third-party libraries.
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"

	// this project.
	"github.com/vanus-labs/vanus/pkg/errors"
)

var (
	_ io.Closer = (*segmentClient)(nil)
	_ Heartbeat = (*segmentClient)(nil)
)

type segmentClient struct {
	cc              *Conn
	heartBeatClient ctrlpb.SegmentController_SegmentHeartbeatClient
}

func NewSegmentClient(cc *Conn) ctrlpb.SegmentControllerClient {
	return &segmentClient{
		cc: cc,
	}
}

func (sc *segmentClient) Beat(ctx context.Context, v interface{}) error {
	log.Debug().Str("leader", sc.cc.leader).Msg("heartbeat")
	req, ok := v.(*ctrlpb.SegmentHeartbeatRequest)
	if !ok {
		return errors.ErrInvalidHeartBeatRequest
	}
	makeSureClient := func() error {
		cli, err := sc.cc.makeSureClient(ctx, false)
		if err != nil {
			return err
		}
		client := ctrlpb.NewSegmentControllerClient(cli)

		sc.heartBeatClient, err = client.SegmentHeartbeat(ctx)
		if err != nil {
			sts := status.Convert(err)
			if sts.Code() == codes.Unavailable {
				cli, err = sc.cc.makeSureClient(ctx, false)
				if err != nil {
					return err
				}
				client = ctrlpb.NewSegmentControllerClient(cli)
				if client == nil {
					return errors.ErrNoControllerLeader
				}
				sc.heartBeatClient, err = client.SegmentHeartbeat(ctx) // TODO panic
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
		if err := makeSureClient(); err != nil {
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
			log.Warn().Err(err).Msg("close heartbeat stream error")
		}
	}
	return sc.cc.close()
}

func (sc *segmentClient) RegisterSegmentServer(
	ctx context.Context, in *ctrlpb.RegisterSegmentServerRequest, opts ...grpc.CallOption,
) (*ctrlpb.RegisterSegmentServerResponse, error) {
	out := new(ctrlpb.RegisterSegmentServerResponse)
	err := sc.cc.invoke(ctx, "/vanus.core.controller.SegmentController/RegisterSegmentServer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) UnregisterSegmentServer(
	ctx context.Context, in *ctrlpb.UnregisterSegmentServerRequest, opts ...grpc.CallOption,
) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	out := new(ctrlpb.UnregisterSegmentServerResponse)
	err := sc.cc.invoke(ctx, "/vanus.core.controller.SegmentController/UnregisterSegmentServer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) ReportSegmentBlockIsFull(
	ctx context.Context, in *ctrlpb.SegmentHeartbeatRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := sc.cc.invoke(ctx, "/vanus.core.controller.SegmentController/ReportSegmentBlockIsFull", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) ReportSegmentLeader(
	ctx context.Context, in *ctrlpb.ReportSegmentLeaderRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := sc.cc.invoke(ctx, "/vanus.core.controller.SegmentController/ReportSegmentLeader", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) QuerySegmentRouteInfo(
	ctx context.Context, in *ctrlpb.QuerySegmentRouteInfoRequest, opts ...grpc.CallOption,
) (*ctrlpb.QuerySegmentRouteInfoResponse, error) {
	out := new(ctrlpb.QuerySegmentRouteInfoResponse)
	err := sc.cc.invoke(ctx, "/vanus.core.controller.SegmentController/QuerySegmentRouteInfo", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (sc *segmentClient) SegmentHeartbeat(_ context.Context, _ ...grpc.CallOption) (ctrlpb.SegmentController_SegmentHeartbeatClient, error) {
	// TODO implement me
	panic("unsupported method, please use controller.RegisterHeartbeat")
}
