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

package metadata

import (
	"fmt"
	"time"

	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/errors"
)

type UserRole struct {
	UserIdentifier string                     `json:"user_identifier"`
	RoleID         string                     `json:"role_id,,omitempty"`
	ResourceID     vanus.ID                   `json:"resource_id,omitempty"`
	ResourceKind   authorization.ResourceKind `json:"resource_kind,omitempty"`
	Role           authorization.Role         `json:"role,omitempty"`
	CreatedAt      time.Time                  `json:"created_at"`
	UpdatedAt      time.Time                  `json:"updated_at"`
}

func (ur *UserRole) GetRoleID() string {
	if ur.RoleID == "" {
		if ur.Role == authorization.RoleClusterAdmin {
			return string(authorization.RoleClusterAdmin)
		}
		return fmt.Sprintf("%s_%s_%s", ur.Role, ur.ResourceKind, ur.ResourceID.Key())
	}
	return ur.RoleID
}

func (ur *UserRole) BuiltIn() bool {
	return ur.RoleID == ""
}

func (ur *UserRole) Validate() error {
	if ur.UserIdentifier == "" {
		return errors.ErrInvalidRequest.WithMessage("user identifier is empty")
	}
	if ur.BuiltIn() { //nolint:nestif // ok
		if !authorization.IsRoleExist(ur.Role) {
			return errors.ErrInvalidRequest.WithMessage("role is invalid")
		}
		if ur.Role == authorization.RoleClusterAdmin {
			if ur.ResourceKind != "" {
				return errors.ErrInvalidRequest.WithMessage("role is clusterAdmin resourceKind is not need")
			}
			if ur.ResourceID != vanus.EmptyID() {
				return errors.ErrInvalidRequest.WithMessage("role is clusterAdmin resourceID is not need")
			}
			return nil
		}
		if !authorization.IsResourceKindExist(ur.ResourceKind) {
			return errors.ErrInvalidRequest.WithMessage("resourceKind is invalid")
		}
		if ur.ResourceKind != authorization.ResourceEventbus &&
			(ur.Role == authorization.RoleRead || ur.Role == authorization.RoleWrite) {
			return errors.ErrInvalidRequest.WithMessage("only eventbus support read or write role")
		}
		if ur.ResourceID == vanus.EmptyID() {
			return errors.ErrInvalidRequest.WithMessage("resourceID is 0")
		}
		return nil
	}
	if ur.Role != "" {
		return errors.ErrInvalidRequest.WithMessage("use custom defined role but role is not empty")
	}
	if ur.ResourceKind != "" {
		return errors.ErrInvalidRequest.WithMessage("use custom defined role but resource_kind is not empty")
	}
	if ur.ResourceID != vanus.EmptyID() {
		return errors.ErrInvalidRequest.WithMessage("use custom defined role but resource_id is not 0")
	}
	return nil
}
