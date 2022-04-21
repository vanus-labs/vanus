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
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	errpb "github.com/linkall-labs/vsproto/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ctrlClient struct {
	ctrlAddrs   []string
	grpcConn    map[string]*grpc.ClientConn
	ctrlClients map[string]ctrlpb.SegmentControllerClient
	mutex       sync.Mutex
	leader      string
	credentials credentials.TransportCredentials
}

func NewClient(ctrlAddrs []string) *ctrlClient {
	return &ctrlClient{
		ctrlAddrs:   ctrlAddrs,
		grpcConn:    map[string]*grpc.ClientConn{},
		ctrlClients: map[string]ctrlpb.SegmentControllerClient{},
		credentials: insecure.NewCredentials(),
	}
}

func (cli *ctrlClient) Close(ctx context.Context) {
	if len(cli.grpcConn) == 0 {
		return
	}
	for ip, conn := range cli.grpcConn {
		if err := conn.Close(); err != nil {
			log.Warning(ctx, "close grpc connection failed", map[string]interface{}{
				log.KeyError:   err,
				"peer_address": ip,
			})
		}
	}
}

func (cli *ctrlClient) registerSegmentServer(ctx context.Context,
	req *ctrlpb.RegisterSegmentServerRequest) (*ctrlpb.RegisterSegmentServerResponse, error) {
	var res *ctrlpb.RegisterSegmentServerResponse
	var err = error(errors.ErrNoControllerLeader)
	for cli.isNeedRetry(err) {
		client := cli.makeSureClient()
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.RegisterSegmentServer(ctx, req)
	}
	return res, err
}

func (cli *ctrlClient) unregisterSegmentServer(ctx context.Context,
	req *ctrlpb.UnregisterSegmentServerRequest) (*ctrlpb.UnregisterSegmentServerResponse, error) {
	var res *ctrlpb.UnregisterSegmentServerResponse
	var err = error(errors.ErrNoControllerLeader)
	for cli.isNeedRetry(err) {
		client := cli.makeSureClient()
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.UnregisterSegmentServer(ctx, req)
	}
	return res, err
}

func (cli *ctrlClient) reportSegmentBlockIsFull(ctx context.Context,
	req *ctrlpb.SegmentHeartbeatRequest) (*emptypb.Empty, error) {
	var res *emptypb.Empty
	var err = error(errors.ErrNoControllerLeader)
	for cli.isNeedRetry(err) {
		client := cli.makeSureClient()
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.ReportSegmentBlockIsFull(ctx, req)
	}
	return res, err
}

func (cli *ctrlClient) heartbeat(ctx context.Context) (ctrlpb.SegmentController_SegmentHeartbeatClient, error) {
	var res ctrlpb.SegmentController_SegmentHeartbeatClient
	var err = error(errors.ErrNoControllerLeader)
	for cli.isNeedRetry(err) {
		client := cli.makeSureClient()
		if client == nil {
			return nil, errors.ErrNoControllerLeader
		}
		res, err = client.SegmentHeartbeat(ctx)
	}
	return res, err
}

func (cli *ctrlClient) makeSureClient() ctrlpb.SegmentControllerClient {
	cli.mutex.Lock()
	defer cli.mutex.Unlock()

	if len(cli.ctrlClients) == 0 || cli.leader == "" {
		return nil
	}

	client := cli.ctrlClients[cli.leader]
	if client == nil {
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(cli.credentials))
		conn, err := grpc.Dial(cli.leader, opts...)
		if err != nil {
			return nil
		}
		cli.grpcConn[cli.leader] = conn
		client = ctrlpb.NewSegmentControllerClient(conn)
		cli.ctrlClients[cli.leader] = client
	}

	return client
}

func (cli *ctrlClient) isNeedRetry(err error) bool {
	if err == errors.ErrNoControllerLeader {
		return true
	}

	errType, ok := errpb.Convert(err)
	if !ok {
		return false
	}

	if errType.Code == errpb.ErrorCode_NOT_LEADER {
		subStr := "please connect to:"
		idx := strings.Index(errType.Description, subStr)
		if idx < 0 {
			return false
		}
		leaderAddress := strings.Trim(errType.Description[idx+len(subStr):], " ")
		ipInfo := strings.Split(leaderAddress, ":")
		if len(ipInfo) != 2 {
			return false
		}
		ip := net.ParseIP(ipInfo[0])
		if ip != nil {
			cli.leader = ip.String()
		} else {
			cli.leader = ""
			return false
		}
		i, err := strconv.Atoi(ipInfo[1])
		if err != nil || i < 2000 || i > 10000 {
			return false
		}
		cli.leader = leaderAddress
		return true
	}
	return false
}
