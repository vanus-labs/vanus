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

type Action string

const (
	UserCreate  Action = "user:create"
	UserDelete  Action = "user:delete"
	UserGet     Action = "user:get"
	UserGetRole Action = "user:getRole"

	TokenCreate Action = "token:create"
	TokenDelete Action = "token:delete"
	TokenGet    Action = "token:get"
	TokenList   Action = "token:list"

	RoleGrant  Action = "role:grant"
	RoleRevoke Action = "role:revoke"

	NamespaceCreate Action = "namespace:create"
	NamespaceDelete Action = "namespace:delete"
	NamespaceGrant  Action = "namespace:grant"
	NamespaceRevoke Action = "namespace:revoke"
	NamespaceGet    Action = "namespace:get"

	EventbusCreate Action = "eventbus:create"
	EventbusUpdate Action = "eventbus:update"
	EventbusGet    Action = "eventbus:get"
	EventbusDelete Action = "eventbus:delete"
	EventbusGrant  Action = "eventbus:grant"
	EventbusRevoke Action = "eventbus:revoke"
	EventbusRead   Action = "eventbus:read"
	EventbusWrite  Action = "eventbus:write"

	SubscriptionCreate Action = "subscription:create"
	SubscriptionUpdate Action = "subscription:update"
	SubscriptionGet    Action = "subscription:get"
	SubscriptionDelete Action = "subscription:delete"
	SubscriptionGrant  Action = "subscription:grant"
	SubscriptionRevoke Action = "subscription:revoke"
)

type List []Action

func (list List) Contains(action Action) bool {
	for _, a := range list {
		if a == action {
			return true
		}
	}
	return false
}
