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

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/controller/tenant/convert"
	"github.com/vanus-labs/vanus/internal/controller/tenant/manager"
	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/kv/etcd"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

var _ ctrlpb.NamespaceControllerServer = &controller{}

func NewController(config Config, mem member.Member) *controller {
	ctrl := &controller{
		config: config,
		member: mem,
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
	id, err := vanus.NewID()
	if err != nil {
		return err
	}
	ns.ID = id
	now := time.Now()
	ns.CreatedAt = now
	ns.UpdatedAt = now
	err = ctrl.namespaceManager.AddNamespace(ctx, ns)
	if err != nil {
		return err
	}
	return nil
}

func (ctrl *controller) ListNamespace(ctx context.Context,
	empty *emptypb.Empty,
) (*ctrlpb.ListNamespaceResponse, error) {
	namespaces := ctrl.namespaceManager.ListNamespace(ctx)
	list := make([]*metapb.Namespace, 0, len(namespaces))
	for i, ns := range namespaces {
		list[i] = convert.ToPbNamespace(ns)
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
	ctrl.member.RegisterMembershipChangedProcessor(ctrl.membershipChangedProcessor)
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
		log.Info(ctx, "the default namespace has been created", map[string]interface{}{
			"namespace": primitive.DefaultNamespace,
		})
	}
	ns = ctrl.namespaceManager.GetNamespaceByName(ctx, primitive.SystemNamespace)
	if ns == nil {
		// create system namespace
		err := ctrl.createNamespace(ctx, &metadata.Namespace{Name: primitive.SystemNamespace})
		if err != nil {
			return err
		}
		log.Info(ctx, "the system namespace has been created", map[string]interface{}{
			"namespace": primitive.SystemNamespace,
		})
	}
	return nil
}

func (ctrl *controller) membershipChangedProcessor(ctx context.Context,
	event member.MembershipChangedEvent,
) error {
	ctrl.membershipMutex.Lock()
	defer ctrl.membershipMutex.Unlock()

	switch event.Type {
	case member.EventBecomeLeader:
		if ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = true
		if err := ctrl.namespaceManager.Init(ctx); err != nil {
			log.Error(ctx, "namespace manager init error", map[string]interface{}{
				log.KeyError: err,
			})
			return err
		}
		if err := ctrl.createSystemNamespace(ctx); err != nil {
			log.Error(ctx, "create system namespace error", map[string]interface{}{
				log.KeyError: err,
			})
			return err
		}
	case member.EventBecomeFollower:
		if !ctrl.isLeader {
			return nil
		}
		ctrl.isLeader = false
		// todo clean
	}
	return nil
}
