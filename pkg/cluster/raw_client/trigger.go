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
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vanus-labs/vanus/observability/log"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"

	"github.com/vanus-labs/vanus/pkg/errors"
)

var (
	_ io.Closer = (*triggerClient)(nil)
	_ Heartbeat = (*triggerClient)(nil)
)

func NewTriggerClient(cc *Conn) ctrlpb.TriggerControllerClient {
	return &triggerClient{
		cc: cc,
	}
}

type triggerClient struct {
	cc              *Conn
	heartBeatClient ctrlpb.TriggerController_TriggerWorkerHeartbeatClient
}

func (tc *triggerClient) SetDeadLetterEventOffset(
	ctx context.Context, in *ctrlpb.SetDeadLetterEventOffsetRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/SetDeadLetterEventOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) GetDeadLetterEventOffset(
	ctx context.Context, in *ctrlpb.GetDeadLetterEventOffsetRequest, opts ...grpc.CallOption,
) (*ctrlpb.GetDeadLetterEventOffsetResponse, error) {
	out := new(ctrlpb.GetDeadLetterEventOffsetResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/GetDeadLetterEventOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) Beat(ctx context.Context, v interface{}) error {
	log.Debug(ctx, "heartbeat", map[string]interface{}{
		"leader": tc.cc.leader,
	})
	req, ok := v.(*ctrlpb.TriggerWorkerHeartbeatRequest)
	if !ok {
		return errors.ErrInvalidHeartBeatRequest
	}
	makeSureClient := func() error {
		gcli, err := tc.cc.makeSureClient(ctx, false)
		if err != nil {
			return err
		}
		client := ctrlpb.NewTriggerControllerClient(gcli)
		tc.heartBeatClient, err = client.TriggerWorkerHeartbeat(ctx)
		if err != nil {
			sts := status.Convert(err)
			if sts.Code() == codes.Unavailable {
				gcli, err := tc.cc.makeSureClient(ctx, false)
				if err != nil {
					return err
				}
				client = ctrlpb.NewTriggerControllerClient(gcli)
				if client == nil {
					return errors.ErrNoControllerLeader
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
		if err := makeSureClient(); err != nil {
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

func (tc *triggerClient) CreateSubscription(
	ctx context.Context, in *ctrlpb.CreateSubscriptionRequest, opts ...grpc.CallOption,
) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/CreateSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) UpdateSubscription(
	ctx context.Context, in *ctrlpb.UpdateSubscriptionRequest, opts ...grpc.CallOption,
) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/UpdateSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) DeleteSubscription(
	ctx context.Context, in *ctrlpb.DeleteSubscriptionRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/DeleteSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) GetSubscription(
	ctx context.Context, in *ctrlpb.GetSubscriptionRequest, opts ...grpc.CallOption,
) (*metapb.Subscription, error) {
	out := new(metapb.Subscription)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/GetSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) ListSubscription(
	ctx context.Context, in *ctrlpb.ListSubscriptionRequest, opts ...grpc.CallOption,
) (*ctrlpb.ListSubscriptionResponse, error) {
	out := new(ctrlpb.ListSubscriptionResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/ListSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) RegisterTriggerWorker(
	ctx context.Context, in *ctrlpb.RegisterTriggerWorkerRequest, opts ...grpc.CallOption,
) (*ctrlpb.RegisterTriggerWorkerResponse, error) {
	out := new(ctrlpb.RegisterTriggerWorkerResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/RegisterTriggerWorker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) UnregisterTriggerWorker(
	ctx context.Context, in *ctrlpb.UnregisterTriggerWorkerRequest, opts ...grpc.CallOption,
) (*ctrlpb.UnregisterTriggerWorkerResponse, error) {
	out := new(ctrlpb.UnregisterTriggerWorkerResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/UnregisterTriggerWorker", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) ResetOffsetToTimestamp(
	ctx context.Context, in *ctrlpb.ResetOffsetToTimestampRequest, opts ...grpc.CallOption,
) (*ctrlpb.ResetOffsetToTimestampResponse, error) {
	out := new(ctrlpb.ResetOffsetToTimestampResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/ResetOffsetToTimestamp", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) CommitOffset(
	ctx context.Context, in *ctrlpb.CommitOffsetRequest, opts ...grpc.CallOption,
) (*ctrlpb.CommitOffsetResponse, error) {
	out := new(ctrlpb.CommitOffsetResponse)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/CommitOffset", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) DisableSubscription(
	ctx context.Context, in *ctrlpb.DisableSubscriptionRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/DisableSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) ResumeSubscription(
	ctx context.Context, in *ctrlpb.ResumeSubscriptionRequest, opts ...grpc.CallOption,
) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := tc.cc.invoke(ctx, "/vanus.core.controller.TriggerController/ResumeSubscription", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (tc *triggerClient) TriggerWorkerHeartbeat(
	_ context.Context, _ ...grpc.CallOption,
) (ctrlpb.TriggerController_TriggerWorkerHeartbeatClient, error) {
	panic("unsupported method, please use controller.RegisterHeartbeat")
}
