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

package segment

import (
	"context"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	errpb "github.com/linkall-labs/vsproto/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type client struct {
	ctrlAddrs   []string
	grpcConn    map[string]grpc.ClientConn
	ctrlClients map[string]ctrlpb.SegmentControllerClient
	mutex       sync.Mutex
	leader      string
}

func NewClient(ctrlAddrs []string) (*client, error) {
	return &client{
		ctrlAddrs:   ctrlAddrs,
		grpcConn:    map[string]grpc.ClientConn{},
		ctrlClients: map[string]ctrlpb.SegmentControllerClient{},
	}, nil
}

func (cli *client) registerSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	var res *ctrlpb.RegisterSegmentServerResponse
	var err = error(errors.ErrNoControllerLeader)
	for cli.isNeedRetry(err) {
		client := cli.makeSureClient(cli.getLeader())
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.RegisterSegmentServer(ctx, req)
	}
	return res, err
}

func (cli *client) unregisterSegmentServer(ctx context.Context,
	in *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	return nil, nil
}

func (cli *client) reportSegmentBlockIsFull(ctx context.Context,
	in *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (cli *client) getLeader() string {
	return ""
}

func (cli *client) makeSureClient(addr string) ctrlpb.SegmentControllerClient {
	return nil
}

func (cli *client) isNeedRetry(err error) bool {
	errType, ok := errpb.Convert(err)
	if !ok {
		return false
	}

	return errType.Code == errpb.ErrorCode_NOT_LEADER
}
