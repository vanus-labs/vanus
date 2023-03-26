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

package authorization

import (
	"context"

	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/cluster"
)

//go:generate mockgen -source=client.go -destination=mock_client.go -package=authorization
type RoleClient interface {
	// IsClusterAdmin check the use is cluster admin.
	IsClusterAdmin(ctx context.Context, user string) (bool, error)
	// GetUserNamespaceID get grant user namespaceID.
	GetUserNamespaceID(ctx context.Context, user string) (vanus.IDList, error)
	// GetUserEventbusID get grant user eventbusID, not contains the eventbus grant namespace.
	GetUserEventbusID(ctx context.Context, user string) (vanus.IDList, error)
	// GetUserSubscriptionID get grant user SubscriptionID, not contains the subscription grant namespace.
	GetUserSubscriptionID(ctx context.Context, user string) (vanus.IDList, error)
	// GetUserRole get user role
	GetUserRole(ctx context.Context, user string) ([]*UserRole, error)
}

var _ RoleClient = &builtInClient{}

type builtInClient struct {
	cluster cluster.Cluster
}

func NewBuiltInClient(cluster cluster.Cluster) RoleClient {
	return &builtInClient{
		cluster: cluster,
	}
}

func (c *builtInClient) GetUserRole(ctx context.Context, user string) ([]*UserRole, error) {
	userRoles, err := c.cluster.AuthService().GetUserRole(ctx, user)
	if err != nil {
		return nil, err
	}
	list := make([]*UserRole, len(userRoles))
	for i := range userRoles {
		list[i] = FromPbUserRole(userRoles[i])
	}
	return list, nil
}

func (c *builtInClient) IsClusterAdmin(ctx context.Context, user string) (bool, error) {
	userRoles, err := c.cluster.AuthService().GetUserRole(ctx, user)
	if err != nil {
		return false, err
	}
	for _, userRole := range userRoles {
		role := FromPbUserRole(userRole)
		if role.IsClusterAdmin() {
			return true, nil
		}
	}
	return false, nil
}

func (c *builtInClient) GetUserNamespaceID(ctx context.Context, user string) (vanus.IDList, error) {
	return c.getUserResourceID(ctx, user, ResourceNamespace)
}

func (c *builtInClient) GetUserEventbusID(ctx context.Context, user string) (vanus.IDList, error) {
	return c.getUserResourceID(ctx, user, ResourceEventbus)
}

func (c *builtInClient) GetUserSubscriptionID(ctx context.Context, user string) (vanus.IDList, error) {
	return c.getUserResourceID(ctx, user, ResourceSubscription)
}

func (c *builtInClient) getUserResourceID(ctx context.Context, user string, kind ResourceKind) (vanus.IDList, error) {
	userRoles, err := c.cluster.AuthService().GetUserRole(ctx, user)
	if err != nil {
		return nil, err
	}
	var ids vanus.IDList
	for _, userRole := range userRoles {
		role := FromPbUserRole(userRole)
		if role.ResourceKind == kind {
			ids = append(ids, role.ResourceID)
		}
	}
	return ids, nil
}
