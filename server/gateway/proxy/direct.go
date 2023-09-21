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
	stdErr "errors"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/api/errors"
	metapb "github.com/vanus-labs/vanus/api/meta"
	proxypb "github.com/vanus-labs/vanus/api/proxy"
	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/client/pkg/api"

	"github.com/vanus-labs/vanus/pkg/authorization"
	"github.com/vanus-labs/vanus/server/gateway/auth"
)

var errMethodNotImplemented = stdErr.New("the method hasn't implemented")

func authCreateEventbus(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.CreateEventbusRequest)).GetNamespaceId())
	return authorization.ResourceNamespace, id, authorization.EventbusCreate
}

func (cp *ControllerProxy) CreateEventbus(
	ctx context.Context, req *ctrlpb.CreateEventbusRequest,
) (*metapb.Eventbus, error) {
	return cp.eventbusCtrl.CreateEventbus(ctx, req)
}

func authDeleteEventbus(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*wrapperspb.UInt64Value)).GetValue())
	return authorization.ResourceEventbus, id, authorization.EventbusDelete
}

func (cp *ControllerProxy) DeleteEventbus(
	ctx context.Context, id *wrapperspb.UInt64Value,
) (*emptypb.Empty, error) {
	return cp.eventbusCtrl.DeleteEventbus(ctx, id)
}

func authGetEventbus(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*wrapperspb.UInt64Value)).GetValue())
	return authorization.ResourceEventbus, id, authorization.EventbusGet
}

func (cp *ControllerProxy) GetEventbus(
	ctx context.Context, id *wrapperspb.UInt64Value,
) (*metapb.Eventbus, error) {
	return cp.eventbusCtrl.GetEventbus(ctx, id)
}

func (cp *ControllerProxy) ListEventbus(
	ctx context.Context, req *ctrlpb.ListEventbusRequest,
) (*ctrlpb.ListEventbusResponse, error) {
	listResp, err := cp.eventbusCtrl.ListEventbus(ctx, req)
	if err != nil {
		return nil, err
	}
	if cp.authService.Disable() {
		return listResp, nil
	}
	user := auth.GetUser(ctx)
	admin, err := cp.authService.GetRoleClient().IsClusterAdmin(ctx, user)
	if err != nil {
		return nil, err
	}
	if admin {
		return listResp, nil
	}
	nsID := vanus.NewIDFromUint64(req.GetNamespaceId())
	namespaceID, err := cp.authService.GetRoleClient().GetUserNamespaceID(ctx, user)
	if err != nil {
		return nil, err
	}
	if nsID != vanus.EmptyID() {
		if !namespaceID.Contains(nsID) {
			return nil, errors.ErrPermissionDenied.WithMessage("no permission access namespace")
		}
		return listResp, nil
	}
	list := make([]*metapb.Eventbus, 0)
	// grant namespace all eventbus
	for _, id := range namespaceID {
		listResp, err = cp.eventbusCtrl.ListEventbus(ctx, &ctrlpb.ListEventbusRequest{
			NamespaceId: id.Uint64(),
		})
		if err != nil {
			return nil, err
		}
		list = append(list, listResp.GetEventbus()...)
	}
	// grant eventbus
	eventbusID, err := cp.authService.GetRoleClient().GetUserEventbusID(ctx, user)
	if err != nil {
		return nil, err
	}
	for _, id := range eventbusID {
		if containsEventbus(list, id) {
			continue
		}
		eb, err := cp.eventbusCtrl.GetEventbus(ctx, wrapperspb.UInt64(id.Uint64()))
		if err != nil {
			if errors.Is(err, errors.ErrResourceNotFound) {
				continue
			}
			return nil, err
		}
		list = append(list, eb)
	}
	return &ctrlpb.ListEventbusResponse{Eventbus: list}, nil
}

func (cp *ControllerProxy) GetEventbusWithHumanFriendly(ctx context.Context,
	request *ctrlpb.GetEventbusWithHumanFriendlyRequest,
) (*metapb.Eventbus, error) {
	return cp.eventbusCtrl.GetEventbusWithHumanFriendly(ctx, request)
}

func (cp *ControllerProxy) UpdateEventbus(
	_ context.Context, _ *ctrlpb.UpdateEventbusRequest,
) (*metapb.Eventbus, error) {
	return nil, errMethodNotImplemented
}

func authListSegment(_ context.Context, req interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.ListSegmentRequest)).GetEventbusId())
	return authorization.ResourceEventbus, id, authorization.EventbusGet
}

func (cp *ControllerProxy) ListSegment(
	ctx context.Context, req *ctrlpb.ListSegmentRequest,
) (*ctrlpb.ListSegmentResponse, error) {
	return cp.eventlogCtrl.ListSegment(ctx, req)
}

func (cp *ControllerProxy) ValidateEventbus(
	ctx context.Context, req *proxypb.ValidateEventbusRequest,
) (*emptypb.Empty, error) {
	err := cp.client.Eventbus(ctx, api.WithID(req.EventbusId)).CheckHealth(ctx)
	return &emptypb.Empty{}, err
}

func authCreateSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.CreateSubscriptionRequest)).GetSubscription().GetNamespaceId())
	return authorization.ResourceNamespace, id, authorization.SubscriptionCreate
}

func (cp *ControllerProxy) CreateSubscription(
	ctx context.Context, req *ctrlpb.CreateSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.CreateSubscription(ctx, req)
}

func authUpdateSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.UpdateSubscriptionRequest)).GetId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionUpdate
}

func (cp *ControllerProxy) UpdateSubscription(
	ctx context.Context, req *ctrlpb.UpdateSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.UpdateSubscription(ctx, req)
}

func authDeleteSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.DeleteSubscriptionRequest)).GetId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionDelete
}

func (cp *ControllerProxy) DeleteSubscription(
	ctx context.Context, req *ctrlpb.DeleteSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.DeleteSubscription(ctx, req)
}

func authGetSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.GetSubscriptionRequest)).GetId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionGet
}

func (cp *ControllerProxy) GetSubscription(
	ctx context.Context, req *ctrlpb.GetSubscriptionRequest,
) (*metapb.Subscription, error) {
	return cp.triggerCtrl.GetSubscription(ctx, req)
}

func (cp *ControllerProxy) ListSubscription(
	ctx context.Context, req *ctrlpb.ListSubscriptionRequest,
) (*ctrlpb.ListSubscriptionResponse, error) {
	listResp, err := cp.triggerCtrl.ListSubscription(ctx, req)
	if err != nil {
		return nil, err
	}
	if cp.authService.Disable() {
		return listResp, nil
	}
	user := auth.GetUser(ctx)
	admin, err := cp.authService.GetRoleClient().IsClusterAdmin(ctx, user)
	if err != nil {
		return nil, err
	}
	if admin {
		return listResp, nil
	}
	nsID := vanus.NewIDFromUint64(req.GetNamespaceId())
	namespaceID, err := cp.authService.GetRoleClient().GetUserNamespaceID(ctx, user)
	if err != nil {
		return nil, err
	}
	if nsID != vanus.EmptyID() {
		if !namespaceID.Contains(nsID) {
			return nil, errors.ErrPermissionDenied.WithMessage("no permission access namespace")
		}
		return listResp, nil
	}
	var list []*metapb.Subscription //nolint:prealloc //ok
	// grant namespace all subscription
	for _, id := range namespaceID {
		listResp, err = cp.triggerCtrl.ListSubscription(ctx, &ctrlpb.ListSubscriptionRequest{
			NamespaceId: id.Uint64(),
			EventbusId:  req.GetEventbusId(),
			Name:        req.GetName(),
		})
		if err != nil {
			return nil, err
		}
		list = append(list, listResp.GetSubscription()...)
	}
	// grant subscription
	subscriptionID, err := cp.authService.GetRoleClient().GetUserSubscriptionID(ctx, user)
	if err != nil {
		return nil, err
	}
	for _, id := range subscriptionID {
		if containsSubscription(list, id) {
			continue
		}
		sub, err := cp.triggerCtrl.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{
			Id: id.Uint64(),
		})
		if err != nil {
			if errors.Is(err, errors.ErrResourceNotFound) {
				continue
			}
			return nil, err
		}
		list = append(list, sub)
	}
	return &ctrlpb.ListSubscriptionResponse{Subscription: list}, nil
}

func authDisableSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.DisableSubscriptionRequest)).GetId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionUpdate
}

func (cp *ControllerProxy) DisableSubscription(
	ctx context.Context, req *ctrlpb.DisableSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.DisableSubscription(ctx, req)
}

func authResumeSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.ResumeSubscriptionRequest)).GetId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionUpdate
}

func (cp *ControllerProxy) ResumeSubscription(
	ctx context.Context, req *ctrlpb.ResumeSubscriptionRequest,
) (*emptypb.Empty, error) {
	return cp.triggerCtrl.ResumeSubscription(ctx, req)
}

func authResetOffsetSubscription(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.ResetOffsetToTimestampRequest)).GetSubscriptionId())
	return authorization.ResourceSubscription, id, authorization.SubscriptionUpdate
}

func (cp *ControllerProxy) ResetOffsetToTimestamp(
	ctx context.Context, req *ctrlpb.ResetOffsetToTimestampRequest,
) (*ctrlpb.ResetOffsetToTimestampResponse, error) {
	return cp.triggerCtrl.ResetOffsetToTimestamp(ctx, req)
}

func (cp *ControllerProxy) GetNamespaceWithHumanFriendly(ctx context.Context,
	value *wrapperspb.StringValue,
) (*metapb.Namespace, error) {
	return cp.nsCtrl.GetNamespaceWithHumanFriendly(ctx, value)
}

func authCreateNamespace(_ context.Context, _ interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.NamespaceCreate
}

func (cp *ControllerProxy) CreateNamespace(ctx context.Context,
	request *ctrlpb.CreateNamespaceRequest,
) (*metapb.Namespace, error) {
	return cp.nsCtrl.CreateNamespace(ctx, request)
}

func (cp *ControllerProxy) ListNamespace(ctx context.Context,
	empty *emptypb.Empty,
) (*ctrlpb.ListNamespaceResponse, error) {
	listResp, err := cp.nsCtrl.ListNamespace(ctx, empty)
	if err != nil {
		return nil, err
	}
	if cp.authService.Disable() {
		return listResp, nil
	}
	user := auth.GetUser(ctx)
	admin, err := cp.authService.GetRoleClient().IsClusterAdmin(ctx, user)
	if err != nil {
		return nil, err
	}
	if admin {
		return listResp, nil
	}
	namespaceID, err := cp.authService.GetRoleClient().GetUserNamespaceID(ctx, user)
	if err != nil {
		return nil, err
	}
	var list []*metapb.Namespace
	for i, ns := range listResp.GetNamespace() {
		id := vanus.NewIDFromUint64(ns.Id)
		if namespaceID.Contains(id) {
			list = append(list, listResp.Namespace[i])
		}
	}
	return &ctrlpb.ListNamespaceResponse{Namespace: list}, nil
}

func authGetNamespace(_ context.Context, req interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.GetNamespaceRequest)).GetId())
	return authorization.ResourceNamespace, id, authorization.NamespaceGet
}

func (cp *ControllerProxy) GetNamespace(ctx context.Context, request *ctrlpb.GetNamespaceRequest,
) (*metapb.Namespace, error) {
	return cp.nsCtrl.GetNamespace(ctx, request)
}

func authDeleteNamespace(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	id := vanus.NewIDFromUint64((req.(*ctrlpb.DeleteNamespaceRequest)).GetId())
	return authorization.ResourceNamespace, id, authorization.NamespaceDelete
}

func (cp *ControllerProxy) DeleteNamespace(ctx context.Context, request *ctrlpb.DeleteNamespaceRequest,
) (*emptypb.Empty, error) {
	return cp.nsCtrl.DeleteNamespace(ctx, request)
}
