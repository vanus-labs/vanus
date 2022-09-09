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
	"github.com/linkall-labs/vanus/observability/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ io.Closer = (*triggerClient)(nil)
	_ Heartbeat = (*triggerClient)(nil)
)

func NewTriggerClient(ctrlAddrs []string, credentials credentials.TransportCredentials) ctrlpb.TriggerControllerClient {
	return &triggerClient{
		cc: newConn(ctrlAddrs, credentials),
	}
}

type triggerClient struct {
	cc              *conn
	heartBeatClient ctrlpb.TriggerController_TriggerWorkerHeartbeatClient
}

func (tc *triggerClient) Beat(ctx context.Context, v interface{}) error {
	log.Debug(ctx, "heartbeat", map[string]interface{}{
		"leader": tc.cc.leader,
	})
	req, ok := v.(*ctrlpb.TriggerWorkerHeartbeatRequest)
	if !ok {
		return ErrInvalidHeartBeatRequest
	}
	var err error
	makeSureClient := func() error {
		client := ctrlpb.NewTriggerControllerClient(tc.cc.makeSureClient(ctx, false))
		tc.heartBeatClient, err = client.TriggerWorkerHeartbeat(ctx)
		if err != nil {
			sts := status.Convert(err)
			if sts.Code() == codes.Unavailable {
				client = ctrlpb.NewTriggerControllerClient(tc.cc.makeSureClient(ctx, true))
				if client == nil {
					return ErrNoControllerLeader
				}
				tc.heartBeatClient, err = client.TriggerWorkerHeartbeat(ctx)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
		return nil
	}

	if tc.heartBeatClient == nil {
		if err = makeSureClient(); err != nil {
			return err
		}
	}

	if err := tc.heartBeatClient.Send(req); err != nil {
		if err = makeSureClient(); err != nil {
			return err
		}
		return err
	}

	return nil
}

func (tc *triggerClient) Close() error {
	if tc.heartBeatClient != nil {
		if err := tc.heartBeatClient.CloseSend(); err != nil {
			log.Warning(context.Background(), "close heartbeat stream error", map[string]interface{}{
				log.KeyError: err,
			})
		}
	}
	return tc.cc.close()
}

func (tc *triggerClient) CreateSubscription(ctx context.Context, in *ctrlpb.CreateSubscriptionRequest,
	opts ...grpc.CallOption) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/CreateSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) UpdateSubscription(ctx context.Context, in *ctrlpb.UpdateSubscriptionRequest,
	opts ...grpc.CallOption) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/UpdateSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) DeleteSubscription(ctx context.Context, in *ctrlpb.DeleteSubscriptionRequest,
	opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/DeleteSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) GetSubscription(ctx context.Context, in *ctrlpb.GetSubscriptionRequest,
	opts ...grpc.CallOption) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/GetSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) ListSubscription(ctx context.Context, in *emptypb.Empty,
	opts ...grpc.CallOption) (*ctrlpb.ListSubscriptionResponse, error) {
	out := new(ctrlpb.ListSubscriptionResponse)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/ListSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) RegisterTriggerWorker(ctx context.Context, in *ctrlpb.RegisterTriggerWorkerRequest,
	opts ...grpc.CallOption) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	out := new(ctrlpb.RegisterTriggerWorkerResponse)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/RegisterTriggerWorker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) UnregisterTriggerWorker(ctx context.Context, in *ctrlpb.UnregisterTriggerWorkerRequest,
	opts ...grpc.CallOption) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	out := new(ctrlpb.UnregisterTriggerWorkerResponse)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/UnregisterTriggerWorker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) ResetOffsetToTimestamp(ctx context.Context, in *ctrlpb.ResetOffsetToTimestampRequest,
	opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/ResetOffsetToTimestamp", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) CommitOffset(ctx context.Context, in *ctrlpb.CommitOffsetRequest,
	opts ...grpc.CallOption) (*ctrlpb.CommitOffsetResponse, error) {
	out := new(ctrlpb.CommitOffsetResponse)
	err := tc.cc.invoke(ctx, "/linkall.vanus.controller.TriggerController/CommitOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) TriggerWorkerHeartbeat(_ context.Context,
	_ ...grpc.CallOption) (ctrlpb.TriggerController_TriggerWorkerHeartbeatClient, error) {
	panic("unsupported method, please use controller.RegisterHeartbeat")
}
