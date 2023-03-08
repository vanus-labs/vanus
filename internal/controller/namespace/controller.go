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

package namespace

import (
	"context"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	_ ctrlpb.NamespaceControllerServer = &controller{}
)

type controller struct {
}

func (c controller) CreateNamespace(ctx context.Context, request *ctrlpb.CreateNamespaceRequest) (*metapb.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (c controller) ListNamespace(ctx context.Context, empty *emptypb.Empty) (*ctrlpb.ListNamespaceResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (c controller) GetNamespace(ctx context.Context, request *ctrlpb.GetNamespaceRequest) (*metapb.Namespace, error) {
	//TODO implement me
	panic("implement me")
}

func (c controller) DeleteNamespace(ctx context.Context, request *ctrlpb.DeleteNamespaceRequest) (*emptypb.Empty, error) {
	//TODO implement me
	panic("implement me")
}
