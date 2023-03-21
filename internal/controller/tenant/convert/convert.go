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

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func FromPbCreateNamespace(ns *ctrlpb.CreateNamespaceRequest) *metadata.Namespace {
	to := &metadata.Namespace{
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
