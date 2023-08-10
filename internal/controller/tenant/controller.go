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

package tenant

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/controller/tenant/convert"
	"github.com/vanus-labs/vanus/internal/controller/tenant/manager"
	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/kv/etcd"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
	"github.com/vanus-labs/vanus/pkg/util"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

var (
	_               ctrlpb.NamespaceControllerServer = &controller{}
	_               ctrlpb.AuthControllerServer      = &controller{}
	tokenRandLength                                  = 32
)

func NewController(config Config, mem member.Member) *controller {
	ctrl := &controller{
		config:  config,
		member:  mem,
		cluster: cluster.NewClusterController(config.ControllerAddr, insecure.NewCredentials()),
	}
	return ctrl
}

type controller struct {
	config           Config
	member           member.Member
	membershipMutex  sync.Mutex
	isLeader         bool
	kvClient         kv.Client
	namespaceManager manager.NamespaceManager
	userManager      manager.UserManager
	tokenManager     manager.TokenManager
	userRoleManager  manager.UserRoleManager
	cluster          cluster.Cluster
}

func (ctrl *controller) CreateUser(ctx context.Context, request *ctrlpb.CreateUserRequest) (*metapb.User, error) {
	if request.GetIdentifier() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	now := time.Now()
	user := &metadata.User{
		Identifier:  request.Identifier,
		Description: request.Description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	err := ctrl.userManager.AddUser(ctx, user)
	if err != nil {
		return nil, err
	}
	return convert.ToPbUser(user), nil
}

func (ctrl *controller) DeleteUser(ctx context.Context, value *wrapperspb.StringValue) (*emptypb.Empty, error) {
	identifier := value.GetValue()
	if identifier == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	err := ctrl.userManager.DeleteUser(ctx, identifier)
	if err != nil {
		return nil, err
	}
	// todo delete token and role
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) ListUser(ctx context.Context, _ *emptypb.Empty) (*ctrlpb.ListUserResponse, error) {
	users := ctrl.userManager.ListUser(ctx)
	list := make([]*metapb.User, len(users))
	for i := range users {
		list[i] = convert.ToPbUser(users[i])
	}
	return &ctrlpb.ListUserResponse{Users: list}, nil
}

func (ctrl *controller) GetUser(ctx context.Context, value *wrapperspb.StringValue) (*metapb.User, error) {
	identifier := value.GetValue()
	if identifier == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	user := ctrl.userManager.GetUser(ctx, identifier)
	if user == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("user not exist")
	}
	return convert.ToPbUser(user), nil
}

func (ctrl *controller) GetUserByToken(ctx context.Context,
	token *wrapperspb.StringValue,
) (*wrapperspb.StringValue, error) {
	if token.GetValue() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("token is empty")
	}
	user, err := ctrl.tokenManager.GetUser(ctx, token.GetValue())
	if err != nil {
		return nil, err
	}
	return wrapperspb.String(user), nil
}

func (ctrl *controller) CreateToken(ctx context.Context,
	request *ctrlpb.CreateTokenRequest,
) (*metapb.Token, error) {
	if request.GetUserIdentifier() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	user := ctrl.userManager.GetUser(ctx, request.GetUserIdentifier())
	if user == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("user identifier no exist")
	}
	id, err := vanus.NewID()
	if err != nil {
		return nil, err
	}
	random, err := util.RandomString(tokenRandLength)
	if err != nil {
		return nil, err
	}
	token := fmt.Sprintf("%s%s", random, id.Key())
	userToken := &metadata.Token{
		ID:             id,
		UserIdentifier: request.UserIdentifier,
		Token:          token,
	}
	now := time.Now()
	userToken.CreatedAt = now
	userToken.UpdatedAt = now
	err = ctrl.tokenManager.AddToken(ctx, userToken)
	if err != nil {
		return nil, err
	}
	return convert.ToPbToken(userToken), nil
}

func (ctrl *controller) DeleteToken(ctx context.Context,
	request *ctrlpb.DeleteTokenRequest,
) (*emptypb.Empty, error) {
	tokenID := vanus.NewIDFromUint64(request.GetId())
	if tokenID == vanus.EmptyID() {
		return nil, errors.ErrInvalidRequest.WithMessage("token id is empty")
	}
	err := ctrl.tokenManager.DeleteToken(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetToken(ctx context.Context, request *wrapperspb.UInt64Value) (*metapb.Token, error) {
	tokenID := vanus.NewIDFromUint64(request.GetValue())
	if tokenID == vanus.EmptyID() {
		return nil, errors.ErrInvalidRequest.WithMessage("token id is empty")
	}
	token, err := ctrl.tokenManager.GetToken(ctx, tokenID)
	if err != nil {
		return nil, err
	}
	return convert.ToPbToken(token), nil
}

func (ctrl *controller) GetUserToken(ctx context.Context,
	request *wrapperspb.StringValue,
) (*ctrlpb.GetTokenResponse, error) {
	if request.GetValue() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	user := ctrl.userManager.GetUser(ctx, request.GetValue())
	if user == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("user identifier no exist")
	}
	tokens := ctrl.tokenManager.GetUserToken(ctx, request.GetValue())
	list := make([]*metapb.Token, len(tokens))
	for i := range tokens {
		list[i] = convert.ToPbToken(tokens[i])
	}
	return &ctrlpb.GetTokenResponse{
		Token: list,
	}, nil
}

func (ctrl *controller) ListToken(ctx context.Context, _ *emptypb.Empty,
) (*ctrlpb.ListTokenResponse, error) {
	tokens := ctrl.tokenManager.ListToken(ctx)
	list := make([]*metapb.Token, len(tokens))
	for i := range tokens {
		list[i] = convert.ToPbToken(tokens[i])
	}
	return &ctrlpb.ListTokenResponse{
		Token: list,
	}, nil
}

func (ctrl *controller) GrantRole(ctx context.Context, request *ctrlpb.RoleRequest) (*emptypb.Empty, error) {
	userRole := convert.FromPbRoleRequest(request)
	err := userRole.Validate()
	if err != nil {
		return nil, err
	}
	user := ctrl.userManager.GetUser(ctx, userRole.UserIdentifier)
	if user == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("user identifier no exist")
	}
	if userRole.RoleID != "" {
		// todo support custom define role
		return nil, errors.ErrResourceCanNotOp.WithMessage("not support custom define role")
	} else if userRole.ResourceID != vanus.EmptyID() {
		switch userRole.ResourceKind {
		case authorization.ResourceNamespace:
			ns, err := ctrl.cluster.NamespaceService().GetNamespace(ctx, userRole.ResourceID.Uint64())
			if err != nil {
				return nil, err
			}
			if ns == nil {
				return nil, errors.ErrResourceNotFound.WithMessage("namespace no exist")
			}
		case authorization.ResourceEventbus:
			eb, err := ctrl.cluster.EventbusService().GetEventbus(ctx, userRole.ResourceID.Uint64())
			if err != nil {
				return nil, err
			}
			if eb == nil {
				return nil, errors.ErrResourceNotFound.WithMessage("eventbus no exist")
			}
		case authorization.ResourceSubscription:
			sub, err := ctrl.cluster.TriggerService().GetSubscription(ctx, userRole.ResourceID.Uint64())
			if err != nil {
				return nil, err
			}
			if sub == nil {
				return nil, errors.ErrResourceNotFound.WithMessage("subscription no exist")
			}
		default:
			return nil, errors.ErrInvalidRequest.WithMessage("resourceKind is invalid")
		}
	}
	now := time.Now()
	userRole.CreatedAt = now
	userRole.UpdatedAt = now
	err = ctrl.userRoleManager.AddUserRole(ctx, userRole)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) RevokeRole(ctx context.Context, request *ctrlpb.RoleRequest) (*emptypb.Empty, error) {
	userRole := convert.FromPbRoleRequest(request)
	err := userRole.Validate()
	if err != nil {
		return nil, err
	}
	err = ctrl.userRoleManager.DeleteUserRole(ctx, userRole)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) GetUserRole(ctx context.Context,
	request *ctrlpb.GetUserRoleRequest,
) (*ctrlpb.GetUserRoleResponse, error) {
	if request.GetUserIdentifier() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	user := ctrl.userManager.GetUser(ctx, request.GetUserIdentifier())
	if user == nil {
		return nil, errors.ErrResourceNotFound.WithMessage("user identifier no exist")
	}
	userRoles, err := ctrl.userRoleManager.GetUserRoleByUser(ctx, request.GetUserIdentifier())
	if err != nil {
		return nil, err
	}
	list := make([]*metapb.UserRole, len(userRoles))
	for i := range userRoles {
		list[i] = convert.ToPbUserRole(userRoles[i])
	}
	return &ctrlpb.GetUserRoleResponse{
		UserRole: list,
	}, nil
}

func (ctrl *controller) GetResourceRole(ctx context.Context,
	request *ctrlpb.GetResourceRoleRequest,
) (*ctrlpb.GetResourceRoleResponse, error) {
	resourceID := vanus.NewIDFromUint64(request.GetId())
	if resourceID == vanus.EmptyID() {
		return nil, errors.ErrInvalidRequest.WithMessage("resource id is 0")
	}
	// todo check resourceID exist
	userRoles, err := ctrl.userRoleManager.GetUserRoleByResourceID(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	list := make([]*metapb.ResourceRole, len(userRoles))
	for i := range userRoles {
		list[i] = convert.ToPbResourceRole(userRoles[i])
	}
	return &ctrlpb.GetResourceRoleResponse{
		ResourceRole: list,
	}, nil
}

func (ctrl *controller) CreateNamespace(ctx context.Context,
	request *ctrlpb.CreateNamespaceRequest,
) (*metapb.Namespace, error) {
	ns := convert.FromPbCreateNamespace(request)
	if ns.Name == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("name is empty")
	}
	_ns := ctrl.namespaceManager.GetNamespaceByName(ctx, ns.Name)
	if _ns != nil {
		return nil, errors.ErrResourceAlreadyExist.WithMessage(fmt.Sprintf("namespace %s exist", ns.Name))
	}
	err := ctrl.createNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}
	return convert.ToPbNamespace(ns), nil
}

func (ctrl *controller) createNamespace(ctx context.Context, ns *metadata.Namespace) error {
	if ns.ID.Equals(vanus.EmptyID()) {
		id, err := vanus.NewID()
		if err != nil {
			return err
		}
		ns.ID = id
	}
	now := time.Now()
	ns.CreatedAt = now
	ns.UpdatedAt = now
	err := ctrl.namespaceManager.AddNamespace(ctx, ns)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *controller) ListNamespace(ctx context.Context,
	_ *emptypb.Empty,
) (*ctrlpb.ListNamespaceResponse, error) {
	namespaces := ctrl.namespaceManager.ListNamespace(ctx)
	list := make([]*metapb.Namespace, len(namespaces))
	for i := range namespaces {
		list[i] = convert.ToPbNamespace(namespaces[i])
	}
	return &ctrlpb.ListNamespaceResponse{
		Namespace: list,
	}, nil
}

func (ctrl *controller) GetNamespace(ctx context.Context,
	request *ctrlpb.GetNamespaceRequest,
) (*metapb.Namespace, error) {
	id := vanus.NewIDFromUint64(request.GetId())
	ns := ctrl.namespaceManager.GetNamespace(ctx, id)
	if ns == nil {
		return nil, errors.ErrResourceNotFound
	}
	return convert.ToPbNamespace(ns), nil
}

func (ctrl *controller) GetNamespaceWithHumanFriendly(ctx context.Context,
	name *wrapperspb.StringValue,
) (*metapb.Namespace, error) {
	if name.GetValue() == "" {
		return nil, errors.ErrInvalidRequest.WithMessage("name is empty")
	}
	ns := ctrl.namespaceManager.GetNamespaceByName(ctx, name.GetValue())
	if ns == nil {
		return nil, errors.ErrResourceNotFound
	}
	return convert.ToPbNamespace(ns), nil
}

func (ctrl *controller) DeleteNamespace(ctx context.Context,
	request *ctrlpb.DeleteNamespaceRequest,
) (*emptypb.Empty, error) {
	id := vanus.NewIDFromUint64(request.GetId())
	err := ctrl.namespaceManager.DeleteNamespace(ctx, id)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (ctrl *controller) Start() error {
	client, err := etcd.NewEtcdClientV3(ctrl.config.Storage.ServerList, ctrl.config.Storage.KeyPrefix)
	if err != nil {
		return err
	}
	ctrl.kvClient = client
	ctrl.namespaceManager = manager.NewNamespaceManager(client)
	ctrl.userManager = manager.NewUserManager(client)
	ctrl.tokenManager = manager.NewTokenManager(client)
	ctrl.userRoleManager = manager.NewUserRoleManager(client)
	ctrl.member.RegisterMembershipChangedProcessor(ctrl.membershipChangedProcessor)
	return nil
}

func (ctrl *controller) init(ctx context.Context) error {
	err := ctrl.createSystemNamespace(ctx)
	if err != nil {
		return err
	}
	err = ctrl.createDefaultUserAndRole(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *controller) createDefaultUserAndRole(ctx context.Context) error {
	now := time.Now()
	user := ctrl.userManager.GetUser(ctx, primitive.DefaultUser)
	if user == nil {
		// create default user
		err := ctrl.userManager.AddUser(ctx, &metadata.User{
			Identifier:  primitive.DefaultUser,
			Description: "default super admin",
			CreatedAt:   now,
			UpdatedAt:   now,
		})
		if err != nil {
			return err
		}
	}
	log.Info(ctx).Msg("the default user has been created")
	tokens := ctrl.tokenManager.GetUserToken(ctx, primitive.DefaultUser)
	if len(tokens) == 0 {
		// create default user token
		id, err := vanus.NewID()
		if err != nil {
			return err
		}
		err = ctrl.tokenManager.AddToken(ctx, &metadata.Token{
			ID:             id,
			UserIdentifier: primitive.DefaultUser,
			Token:          primitive.DefaultUser,
			CreatedAt:      now,
			UpdatedAt:      now,
		})
		if err != nil {
			return err
		}
	}
	log.Info(ctx).Msg("the default user token has been created")
	userRole := &metadata.UserRole{
		UserIdentifier: primitive.DefaultUser,
		Role:           authorization.RoleClusterAdmin,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	exist := ctrl.userRoleManager.IsUserRoleExist(ctx, userRole)
	if !exist {
		// create default user role
		err := ctrl.userRoleManager.AddUserRole(ctx, userRole)
		if err != nil {
			return err
		}
	}
	log.Info(ctx).Msg("the default user role has been created")
	return nil
}

func (ctrl *controller) createSystemNamespace(ctx context.Context) error {
	ns := ctrl.namespaceManager.GetNamespaceByName(ctx, primitive.DefaultNamespace)
	if ns == nil {
		// create default namespace
		err := ctrl.createNamespace(ctx, &metadata.Namespace{Name: primitive.DefaultNamespace})
		if err != nil {
			return err
		}

		log.Info(ctx).
			Str("namespace", primitive.DefaultNamespace).
			Msg("the default namespace has been created")
	}
	ns = ctrl.namespaceManager.GetNamespaceByName(ctx, primitive.SystemNamespace)
	if ns == nil {
		// create system namespace
		err := ctrl.createNamespace(ctx, &metadata.Namespace{Name: primitive.SystemNamespace})
		if err != nil {
			return err
		}
		log.Info(ctx).
			Str("namespace", primitive.DefaultNamespace).
			Msg("the system namespace has been created")
	}
	return nil
}

func (ctrl *controller) membershipChangedProcessor(ctx context.Context,
	event member.MembershipChangedEvent,
) error {
	log.Info(ctx).
		Interface("event", event).
		Msg("start to process membership change event")
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()

	switch event.Type {
	case member.EventBecomeLeader:
		if ctrl.isLeader {
			log.Info(ctx).Msg("I am leader")
			return nil
		}
		ctrl.isLeader = true
		if err := ctrl.namespaceManager.Init(ctx); err != nil {
			log.Error(ctx).
				Err(err).
				Msg("namespace manager init error")
			return err
		}

		if err := ctrl.userManager.Init(ctx); err != nil {
			log.Error(ctx).Err(err).Msg("user manager init error")
			return err
		}
		if err := ctrl.tokenManager.Init(ctx); err != nil {
			log.Error(ctx).Err(err).Msg("token manager init error")
			return err
		}
		if err := ctrl.userRoleManager.Init(ctx); err != nil {
			log.Error(ctx).Err(err).Msg("user role manager init error")
			return err
		}
		if err := ctrl.init(ctx); err != nil {
			log.Error(ctx).Err(err).Msg("controller init error")
		}

		if err := ctrl.createSystemNamespace(ctx); err != nil {
			log.Error(ctx).
				Err(err).
				Msg("create system namespace error")
			return err
		}
		log.Info(ctx).Msg("the controller init success")
	case member.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = false
		log.Info(ctx).Msg("the controller lost leader")
	}
	return nil
}
