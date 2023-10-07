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
	"fmt"
)

type ResourceKind string

const (
	ResourceUnknown      ResourceKind = "unknown" // no need authorization
	ResourceCluster      ResourceKind = "cluster"
	ResourceNamespace    ResourceKind = "namespace"
	ResourceEventbus     ResourceKind = "eventbus"
	ResourceSubscription ResourceKind = "subscription"
)

type Role string

const (
	RoleClusterAdmin Role = "clusterAdmin"
	RoleAdmin        Role = "admin"
	RoleEdit         Role = "edit"
	RoleView         Role = "view"
	RoleRead         Role = "read"
	RoleWrite        Role = "write"
)

var (
	allRole     = map[Role]struct{}{}
	allResource = map[ResourceKind]struct{}{}
)

func init() { //nolint:gochecknoinits // ok
	allRole[RoleAdmin] = struct{}{}
	allRole[RoleEdit] = struct{}{}
	allRole[RoleView] = struct{}{}
	allRole[RoleClusterAdmin] = struct{}{}
	allRole[RoleRead] = struct{}{}
	allRole[RoleWrite] = struct{}{}

	allResource[ResourceNamespace] = struct{}{}
	allResource[ResourceEventbus] = struct{}{}
	allResource[ResourceSubscription] = struct{}{}
}

func IsRoleExist(role Role) bool {
	_, exist := allRole[role]
	return exist
}

func IsResourceKindExist(kind ResourceKind) bool {
	_, exist := allResource[kind]
	return exist
}

type resourceKindRole string

var (
	namespaceAdmin    = makeResourceKindRole(ResourceNamespace, RoleAdmin)
	namespaceEdit     = makeResourceKindRole(ResourceNamespace, RoleEdit)
	namespaceView     = makeResourceKindRole(ResourceNamespace, RoleView)
	eventbusAdmin     = makeResourceKindRole(ResourceEventbus, RoleAdmin)
	eventbusEdit      = makeResourceKindRole(ResourceEventbus, RoleAdmin)
	eventbusView      = makeResourceKindRole(ResourceEventbus, RoleAdmin)
	eventbusRead      = makeResourceKindRole(ResourceEventbus, RoleAdmin)
	eventbusWrite     = makeResourceKindRole(ResourceEventbus, RoleAdmin)
	subscriptionAdmin = makeResourceKindRole(ResourceSubscription, RoleAdmin)
	subscriptionEdit  = makeResourceKindRole(ResourceSubscription, RoleAdmin)
	subscriptionView  = makeResourceKindRole(ResourceSubscription, RoleAdmin)
)

var builtInRole map[resourceKindRole]map[Action]struct{}

func init() { //nolint:gochecknoinits // ok
	builtInRole = make(map[resourceKindRole]map[Action]struct{})
	addActionForRole(NamespaceGet, namespaceAdmin, namespaceEdit, namespaceView)
	addActionForRole(NamespaceGrant, namespaceAdmin)
	addActionForRole(NamespaceRevoke, namespaceAdmin)
	addActionForRole(EventbusCreate, namespaceAdmin, namespaceEdit, eventbusAdmin)
	addActionForRole(EventbusDelete, namespaceAdmin, namespaceEdit)
	addActionForRole(EventbusGrant, namespaceAdmin, namespaceEdit, eventbusAdmin)
	addActionForRole(EventbusRevoke, namespaceAdmin, namespaceEdit, eventbusAdmin)
	addActionForRole(EventbusUpdate, namespaceAdmin, namespaceEdit, eventbusAdmin, eventbusEdit)
	addActionForRole(EventbusGet, namespaceAdmin, namespaceEdit, namespaceView, eventbusAdmin, eventbusEdit, eventbusView)
	addActionForRole(EventbusRead, namespaceAdmin, namespaceEdit, eventbusAdmin, eventbusEdit, eventbusView, eventbusRead)
	addActionForRole(EventbusWrite, namespaceAdmin, namespaceEdit, eventbusAdmin, eventbusEdit, eventbusWrite)
	addActionForRole(SubscriptionCreate, namespaceAdmin, namespaceEdit, subscriptionAdmin)
	addActionForRole(SubscriptionDelete, namespaceAdmin, namespaceEdit)
	addActionForRole(SubscriptionGrant, namespaceAdmin, namespaceEdit, subscriptionAdmin)
	addActionForRole(SubscriptionRevoke, namespaceAdmin, namespaceEdit, subscriptionAdmin)
	addActionForRole(SubscriptionUpdate, namespaceAdmin, namespaceEdit, subscriptionAdmin, subscriptionEdit)
	addActionForRole(SubscriptionGet, namespaceAdmin, namespaceEdit, namespaceView, subscriptionAdmin, subscriptionEdit, subscriptionView) //nolint:lll // ok
}

func addActionForRole(_action Action, roles ...resourceKindRole) {
	for _, role := range roles {
		actions, ok := builtInRole[role]
		if !ok {
			actions = map[Action]struct{}{}
			builtInRole[role] = actions
		}
		actions[_action] = struct{}{}
	}
}

func hasAction(kind ResourceKind, role Role, _action Action) bool {
	actions, ok := builtInRole[makeResourceKindRole(kind, role)]
	if !ok {
		return false
	}
	_, ok = actions[_action]
	return ok
}

func makeResourceKindRole(kind ResourceKind, role Role) resourceKindRole {
	return resourceKindRole(fmt.Sprintf("%s-%s", kind, role))
}
