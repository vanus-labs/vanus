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

package cluster

import (
	"context"

	"github.com/vanus-labs/vanus/pkg/cluster/raw_client"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	systemNamespace  = "vanus-system"
	defaultNamespace = "default"
)

type namespaceService struct {
	client ctrlpb.NamespaceControllerClient
}

func (ns *namespaceService) GetNamespace(ctx context.Context, id uint64) (*metapb.Namespace, error) {
	return ns.client.GetNamespace(ctx, &ctrlpb.GetNamespaceRequest{Id: id})
}

func (ns *namespaceService) GetSystemNamespace(ctx context.Context) (*metapb.Namespace, error) {
	return ns.client.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(systemNamespace))
}

func (ns *namespaceService) GetDefaultNamespace(ctx context.Context) (*metapb.Namespace, error) {
	return ns.client.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(defaultNamespace))
}

func (ns *namespaceService) GetNamespaceByName(ctx context.Context, name string) (*metapb.Namespace, error) {
	return nil, nil
}

func newNamespaceService(cc *raw_client.Conn) NamespaceService {
	return &namespaceService{client: raw_client.NewNamespaceClient(cc)}
}

func (ns *namespaceService) RawClient() ctrlpb.NamespaceControllerClient {
	return ns.client
}
