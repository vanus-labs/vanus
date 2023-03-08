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

package role

type Action string

const (
	NamespaceAll    Action = "namespace:*"
	NamespaceCreate Action = "namespace:create"
	NamespaceDelete Action = "namespace:delete"
	NamespaceGrant  Action = "namespace:grant"
	NamespaceRevoke Action = "namespace:revoke"
	NamespaceGet    Action = "namespace:get"

	EventbusAll    Action = "eventbus:*"
	EventbusCreate Action = "eventbus:create"
	EventbusUpdate Action = "eventbus:update"
	EventbusGet    Action = "eventbus:get"
	EventbusDelete Action = "eventbus:delete"
	EventbusGrant  Action = "eventbus:grant"
	EventbusRevoke Action = "eventbus:revoke"
	EventbusRead   Action = "eventbus:read"
	EventbusWrite  Action = "eventbus:write"

	SubscriptionAll    Action = "subscription:*"
	SubscriptionCreate Action = "subscription:create"
	SubscriptionUpdate Action = "subscription:update"
	SubscriptionGet    Action = "subscription:get"
	SubscriptionDelete Action = "subscription:delete"
	SubscriptionGrant  Action = "subscription:grant"
	SubscriptionRevoke Action = "subscription:revoke"
)

type ActionList []Action

func (list ActionList) Contains(action Action) bool {
	for _, a := range list {
		if a == action {
			return true
		}
	}
	return false
}

type ActionGroup string

const (
	ClusterAdmin      ActionGroup = "clusterAdmin"
	NamespaceAdmin    ActionGroup = "namespaceAdmin"
	NamespaceEdit     ActionGroup = "namespaceEdit"
	NamespaceView     ActionGroup = "namespaceView"
	EventbusAdmin     ActionGroup = "eventbusAdmin"
	EventbusEdit      ActionGroup = "eventbusEdit"
	EventbusView      ActionGroup = "eventbusView"
	EventbusReader    ActionGroup = "eventbusRead"
	EventbusWriter    ActionGroup = "eventbusWrite"
	SubscriptionAdmin ActionGroup = "subscriptionAdmin"
	SubscriptionEdit  ActionGroup = "subscriptionEdit"
	SubscriptionView  ActionGroup = "subscriptionView"
)

var SystemActionList map[ActionGroup]map[Action]struct{}

func init() {
	SystemActionList = make(map[ActionGroup]map[Action]struct{})
	addActionForGroup(EventbusAll)
	SystemActionList[NamespaceAdmin] = map[Action]struct{}{
		NamespaceGrant:  {},
		NamespaceRevoke: {},
		NamespaceGet:    {},
		EventbusAll:     {},
		SubscriptionAll: {},
	}
}

func addActionForGroup(action Action, groups ...ActionGroup) {
	for _, group := range groups {
		actions, ok := SystemActionList[group]
		if !ok {
			actions = map[Action]struct{}{}
			SystemActionList[group] = actions
		}
		actions[action] = struct{}{}
	}
}
