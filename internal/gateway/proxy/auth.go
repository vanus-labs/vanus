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

package proxy

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/gateway/auth"
	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func authCreateUser(_ context.Context, _ interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.UserCreate
}

func (cp *ControllerProxy) CreateUser(ctx context.Context, request *ctrlpb.CreateUserRequest) (*metapb.User, error) {
	return cp.authCtrl.CreateUser(ctx, request)
}

func authDeleteUser(_ context.Context, _ interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.UserDelete
}

func (cp *ControllerProxy) DeleteUser(ctx context.Context, value *wrapperspb.StringValue) (*emptypb.Empty, error) {
	return cp.authCtrl.DeleteUser(ctx, value)
}

func (cp *ControllerProxy) GetUser(ctx context.Context, value *wrapperspb.StringValue) (*metapb.User, error) {
	return cp.authCtrl.GetUser(ctx, value)
}

func (cp *ControllerProxy) ListUser(ctx context.Context, empty *emptypb.Empty) (*ctrlpb.ListUserResponse, error) {
	return cp.authCtrl.ListUser(ctx, empty)
}

func (cp *ControllerProxy) checkUserIdentifier(ctx context.Context, identifier string) (string, error) {
	if cp.authService.Disable() {
		return identifier, nil
	}
	user := auth.GetUser(ctx)
	if identifier == "" || user == identifier {
		return user, nil
	}
	// check role
	admin, err := cp.authService.GetRoleClient().IsClusterAdmin(ctx, user)
	if err != nil {
		return "", err
	}
	if !admin {
		return "", errors.ErrPermissionDenied.WithMessage("no permission")
	}
	return identifier, nil
}

func (cp *ControllerProxy) GetUserToken(ctx context.Context, request *wrapperspb.StringValue,
) (*ctrlpb.GetTokenResponse, error) {
	identifier, err := cp.checkUserIdentifier(ctx, request.GetValue())
	if err != nil {
		return nil, err
	}
	return cp.authCtrl.GetUserToken(ctx, wrapperspb.String(identifier))
}

func authListToken(_ context.Context, _ interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.TokenList
}

func (cp *ControllerProxy) ListToken(ctx context.Context, empty *emptypb.Empty) (*ctrlpb.ListTokenResponse, error) {
	return cp.authCtrl.ListToken(ctx, empty)
}

func (cp *ControllerProxy) CreateToken(ctx context.Context, request *ctrlpb.CreateTokenRequest,
) (*metapb.Token, error) {
	identifier, err := cp.checkUserIdentifier(ctx, request.UserIdentifier)
	if err != nil {
		return nil, err
	}
	return cp.authCtrl.CreateToken(ctx, &ctrlpb.CreateTokenRequest{UserIdentifier: identifier})
}

func (cp *ControllerProxy) DeleteToken(ctx context.Context, request *ctrlpb.DeleteTokenRequest,
) (*emptypb.Empty, error) {
	if cp.authService.Disable() {
		return cp.authCtrl.DeleteToken(ctx, request)
	}
	token, err := cp.authCtrl.GetToken(ctx, wrapperspb.UInt64(request.GetId()))
	if err != nil {
		return nil, err
	}
	user := auth.GetUser(ctx)
	if token.GetUserIdentifier() != user {
		admin, err := cp.authService.GetRoleClient().IsClusterAdmin(ctx, user)
		if err != nil {
			return nil, err
		}
		if !admin {
			return nil, errors.ErrPermissionDenied.WithMessage("no permission")
		}
	}
	return cp.authCtrl.DeleteToken(ctx, request)
}

func authGrantRole(_ context.Context, req interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	in, _ := req.(*ctrlpb.RoleRequest)
	id := vanus.NewIDFromUint64(in.ResourceId)
	kind := authorization.ResourceKind(in.ResourceKind)
	switch kind {
	case authorization.ResourceNamespace:
		return authorization.ResourceNamespace, id, authorization.NamespaceGrant
	case authorization.ResourceEventbus:
		return authorization.ResourceEventbus, id, authorization.EventbusGrant
	case authorization.ResourceSubscription:
		return authorization.ResourceSubscription, id, authorization.SubscriptionGrant
	}
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.RoleGrant
}

func (cp *ControllerProxy) GrantRole(ctx context.Context, request *ctrlpb.RoleRequest) (*emptypb.Empty, error) {
	if request.ResourceKind != "" {
		resourceKind := authorization.ResourceKind(request.ResourceKind)
		switch resourceKind {
		case authorization.ResourceNamespace:
		case authorization.ResourceEventbus:
			if !cp.authService.OpenEventbus() {
				return nil, errors.ErrResourceCanNotOp.WithMessage("not support eventbus")
			}
		case authorization.ResourceSubscription:
			if !cp.authService.OpenSubscription() {
				return nil, errors.ErrResourceCanNotOp.WithMessage("not support subscription")
			}
		default:
			return nil, errors.ErrInvalidRequest.WithMessage("resourceKind invalid")
		}
	}
	return cp.authCtrl.GrantRole(ctx, request)
}

func authRevokeRole(_ context.Context, req interface{}) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	in, _ := req.(*ctrlpb.RoleRequest)
	id := vanus.NewIDFromUint64(in.ResourceId)
	kind := authorization.ResourceKind(in.ResourceKind)
	switch kind {
	case authorization.ResourceNamespace:
		return authorization.ResourceNamespace, id, authorization.NamespaceRevoke
	case authorization.ResourceEventbus:
		return authorization.ResourceEventbus, id, authorization.EventbusRevoke
	case authorization.ResourceSubscription:
		return authorization.ResourceSubscription, id, authorization.SubscriptionRevoke
	}
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.RoleRevoke
}

func (cp *ControllerProxy) RevokeRole(ctx context.Context, request *ctrlpb.RoleRequest) (*emptypb.Empty, error) {
	return cp.authCtrl.RevokeRole(ctx, request)
}

func (cp *ControllerProxy) GetUserRole(ctx context.Context, request *ctrlpb.GetUserRoleRequest,
) (*ctrlpb.GetUserRoleResponse, error) {
	identifier, err := cp.checkUserIdentifier(ctx, request.GetUserIdentifier())
	if err != nil {
		return nil, err
	}
	return cp.authCtrl.GetUserRole(ctx, &ctrlpb.GetUserRoleRequest{UserIdentifier: identifier})
}

func authGetResourceRole(_ context.Context, req interface{},
) (authorization.ResourceKind, vanus.ID, authorization.Action) {
	in, _ := req.(*ctrlpb.GetResourceRoleRequest)
	id := vanus.NewIDFromUint64(in.GetId())
	kind := authorization.ResourceKind(in.Kind)
	switch kind {
	case authorization.ResourceNamespace:
		return authorization.ResourceNamespace, id, authorization.NamespaceGrant
	case authorization.ResourceEventbus:
		return authorization.ResourceEventbus, id, authorization.EventbusGrant
	case authorization.ResourceSubscription:
		return authorization.ResourceSubscription, id, authorization.SubscriptionGrant
	}
	// resource kind invalid
	return authorization.ResourceCluster, vanus.EmptyID(), authorization.RoleGrant
}

func (cp *ControllerProxy) GetResourceRole(ctx context.Context,
	request *ctrlpb.GetResourceRoleRequest,
) (*ctrlpb.GetResourceRoleResponse, error) {
	return cp.authCtrl.GetResourceRole(ctx, request)
}
