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

package proxy

import (
	"context"
	"errors"

	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

var errMethodNotImplemented = errors.New("the method hasn't implemented")

func (cp *ControllerProxy) CreateEventbus(
	ctx context.Context, req *ctrlpb.CreateEventbusRequest,
) (*metapb.Eventbus, error) {
	return cp.eventbusCtrl.CreateEventbus(ctx, req)
}

func (cp *ControllerProxy) DeleteEventbus(
	ctx context.Context, req *metapb.Eventbus,
) (*emptypb.Empty, error) {
	return cp.eventbusCtrl.DeleteEventbus(ctx, req)
}

func (cp *ControllerProxy) GetEventbus(
	ctx context.Context, req *metapb.Eventbus,
) (*metapb.Eventbus, error) {
	return cp.eventbusCtrl.GetEventbus(ctx, req)
}

func (cp *ControllerProxy) ListEventbus(
	ctx context.Context, req *emptypb.Empty,
) (*ctrlpb.ListEventbusResponse, error) {
	return cp.eventbusCtrl.ListEventbus(ctx, req)
}

func (cp *ControllerProxy) UpdateEventbus(
	_ context.Context, _ *ctrlpb.UpdateEventbusRequest,
) (*metapb.Eventbus, error) {
	return nil, errMethodNotImplemented
}

func (cp *ControllerProxy) ListSegment(
	ctx context.Context, req *ctrlpb.ListSegmentRequest,
) (*ctrlpb.ListSegmentResponse, error) {
	return cp.eventlogCtrl.ListSegment(ctx, req)
}

func (cp *ControllerProxy) CreateSubscription(
	ctx context.Context, req *ctrlpb.CreateSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.CreateSubscription(ctx, req)
}

func (cp *ControllerProxy) UpdateSubscription(
	ctx context.Context, req *ctrlpb.UpdateSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.UpdateSubscription(ctx, req)
}

func (cp *ControllerProxy) DeleteSubscription(
	ctx context.Context, req *ctrlpb.DeleteSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.DeleteSubscription(ctx, req)
}

func (cp *ControllerProxy) GetSubscription(
	ctx context.Context, req *ctrlpb.GetSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.GetSubscription(ctx, req)
}

func (cp *ControllerProxy) ListSubscription(
	ctx context.Context, req *ctrlpb.ListSubscriptionRequest,
) (*ctrlpb.ListSubscriptionResponse, error) {
	return cp.triggerCtrl.ListSubscription(ctx, req)
}

func (cp *ControllerProxy) DisableSubscription(
	ctx context.Context, req *ctrlpb.DisableSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.DisableSubscription(ctx, req)
}

func (cp *ControllerProxy) ResumeSubscription(
	ctx context.Context, req *ctrlpb.ResumeSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.ResumeSubscription(ctx, req)
}

func (cp *ControllerProxy) ResetOffsetToTimestamp(
	ctx context.Context, req *ctrlpb.ResetOffsetToTimestampRequest,
) (*ctrlpb.ResetOffsetToTimestampResponse, error) {
	return cp.triggerCtrl.ResetOffsetToTimestamp(ctx, req)
}

func (cp *ControllerProxy) SetDeadLetterEventOffset(
	ctx context.Context, req *ctrlpb.SetDeadLetterEventOffsetRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.SetDeadLetterEventOffset(ctx, req)
}
