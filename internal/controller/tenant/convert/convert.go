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

package convert

import (
	"github.com/vanus-labs/vanus/internal/controller/tenant/metadata"
	"github.com/vanus-labs/vanus/internal/primitive/authorization"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func FromPbCreateNamespace(ns *ctrlpb.CreateNamespaceRequest) *metadata.Namespace {
	to := &metadata.Namespace{
		ID:          vanus.NewIDFromUint64(ns.Id),
		Name:        ns.Name,
		Description: ns.Description,
	}
	return to
}

func ToPbNamespace(ns *metadata.Namespace) *metapb.Namespace {
	to := &metapb.Namespace{
		Id:          ns.ID.Uint64(),
		Name:        ns.Name,
		Description: ns.Description,
		CreatedAt:   ns.CreatedAt.UnixMilli(),
		UpdatedAt:   ns.UpdatedAt.UnixMilli(),
	}
	return to
}

func ToPbUser(from *metadata.User) *metapb.User {
	to := &metapb.User{
		Identifier:  from.Identifier,
		Description: from.Description,
		CreatedAt:   from.CreatedAt.UnixMilli(),
		UpdatedAt:   from.UpdatedAt.UnixMilli(),
	}
	return to
}

func ToPbToken(token *metadata.Token) *metapb.Token {
	to := &metapb.Token{
		Id:             token.ID.Uint64(),
		Token:          token.Token,
		UserIdentifier: token.UserIdentifier,
		CreatedAt:      token.CreatedAt.UnixMilli(),
		UpdatedAt:      token.UpdatedAt.UnixMilli(),
	}
	return to
}

func FromPbRoleRequest(from *ctrlpb.RoleRequest) *metadata.UserRole {
	to := &metadata.UserRole{
		UserIdentifier: from.UserIdentifier,
		RoleID:         from.RoleId,
		Role:           authorization.Role(from.RoleName),
		ResourceKind:   authorization.ResourceKind(from.ResourceKind),
		ResourceID:     vanus.NewIDFromUint64(from.ResourceId),
	}
	return to
}

func ToPbUserRole(from *metadata.UserRole) *metapb.UserRole {
	to := &metapb.UserRole{
		UserIdentifier: from.UserIdentifier,
		RoleName:       string(from.Role),
		ResourceKind:   string(from.ResourceKind),
		ResourceId:     from.ResourceID.Uint64(),
		BuiltIn:        from.BuiltIn(),
		CreatedAt:      from.CreatedAt.UnixMilli(),
	}
	return to
}

func ToPbResourceRole(from *metadata.UserRole) *metapb.ResourceRole {
	to := &metapb.ResourceRole{
		UserIdentifier: from.UserIdentifier,
		RoleName:       string(from.Role),
		ResourceKind:   string(from.ResourceKind),
		ResourceId:     from.ResourceID.Uint64(),
		BuiltIn:        from.BuiltIn(),
		CreatedAt:      from.CreatedAt.UnixMilli(),
	}
	return to
}
