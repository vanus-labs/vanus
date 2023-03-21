// Copyright 2022 Linkall Inc.
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

package raw_client

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

var _ io.Closer = (*namespaceClient)(nil)

func NewNamespaceClient(cc *Conn) ctrlpb.NamespaceControllerClient {
	return &namespaceClient{
		cc: cc,
	}
}

type namespaceClient struct {
	cc *Conn
}

func (elc *namespaceClient) CreateNamespace(ctx context.Context, in *ctrlpb.CreateNamespaceRequest, opts ...grpc.CallOption) (*metapb.Namespace, error) {
	out := new(metapb.Namespace)
	err := elc.cc.invoke(ctx, ctrlpb.NamespaceController_CreateNamespace_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *namespaceClient) ListNamespace(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*ctrlpb.ListNamespaceResponse, error) {
	out := new(ctrlpb.ListNamespaceResponse)
	err := elc.cc.invoke(ctx, ctrlpb.NamespaceController_ListNamespace_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *namespaceClient) GetNamespace(ctx context.Context, in *ctrlpb.GetNamespaceRequest, opts ...grpc.CallOption) (*metapb.Namespace, error) {
	out := new(metapb.Namespace)
	err := elc.cc.invoke(ctx, ctrlpb.NamespaceController_GetNamespace_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *namespaceClient) DeleteNamespace(ctx context.Context, in *ctrlpb.DeleteNamespaceRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := elc.cc.invoke(ctx, ctrlpb.NamespaceController_DeleteNamespace_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *namespaceClient) GetNamespaceWithHumanFriendly(ctx context.Context, in *wrapperspb.StringValue, opts ...grpc.CallOption) (*metapb.Namespace, error) {
	out := new(metapb.Namespace)
	err := elc.cc.invoke(ctx, ctrlpb.NamespaceController_GetNamespaceWithHumanFriendly_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (elc *namespaceClient) Close() error {
	return elc.cc.close()
}
