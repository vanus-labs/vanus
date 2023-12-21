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

//go:generate mockgen -source=authorization.go -destination=mock_authorization.go -package=authorization
package authorization

import (
	"context"

	"github.com/vanus-labs/vanus/api/cluster"
	vanus "github.com/vanus-labs/vanus/api/vsr"
)

type Authorization interface {
	Authorize(ctx context.Context, user string, attributes Attributes) (bool, error)
}

var _ Authorization = &authorization{}

type authorization struct {
	client  RoleClient
	cluster cluster.Cluster
}

func NewAuthorization(client RoleClient, cluster cluster.Cluster) Authorization {
	return &authorization{
		cluster: cluster,
		client:  client,
	}
}

func (a *authorization) Authorize(ctx context.Context, user string, attributes Attributes) (bool, error) {
	isClusterAdmin, err := a.client.IsClusterAdmin(ctx, user)
	if err != nil {
		return false, err
	}
	if isClusterAdmin {
		return true, nil
	}
	userRoles, err := a.client.GetUserRole(ctx, user)
	if err != nil {
		return false, err
	}
	if hasPermission(userRoles, attributes, attributes.GetResourceID()) {
		return true, nil
	}
	if attributes.GetResourceID() == vanus.EmptyID() {
		return false, nil
	}
	var resourceID vanus.ID
	switch attributes.GetResourceKind() {
	case ResourceEventbus:
		eb, err := a.cluster.EventbusService().GetEventbus(ctx, attributes.GetResourceID().Uint64())
		if err != nil || eb == nil {
			return false, err
		}
		resourceID = vanus.NewIDFromUint64(eb.NamespaceId)
	case ResourceSubscription:
		sub, err := a.cluster.TriggerService().GetSubscription(ctx, attributes.GetResourceID().Uint64())
		if err != nil || sub == nil {
			return false, err
		}
		resourceID = vanus.NewIDFromUint64(sub.NamespaceId)
	default:
		return false, nil
	}
	return hasPermission(userRoles, attributes, resourceID), nil
}

// hasPermission is loop all user role to check has permission
// todo optimize use role compare with resource action role
func hasPermission(roles []*UserRole, attributes Attributes, resourceID vanus.ID) bool {
	for _, role := range roles {
		if role.BuiltIn {
			if role.ResourceID != resourceID {
				continue
			}
			if hasAction(role.ResourceKind, role.Role, attributes.GetAction()) {
				return true
			}
			continue
		}
		// todo custom role
	}
	return false
}
