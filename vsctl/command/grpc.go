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

package command

import (
	"context"
	"time"

	"github.com/fatih/color"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func mustGetGRPCConn(ctx context.Context,
	cmd *cobra.Command) *grpc.ClientConn {
	endpoints := mustEndpointsFromCmd(cmd)
	connMap := make(map[string]*grpc.ClientConn, 0)
	var leaderAddr string
	for idx := range endpoints {
		conn := createGRPCConn(ctx, endpoints[idx])
		connMap[endpoints[idx]] = nil
		if conn == nil {
			continue
		}
		pingClient := ctrlpb.NewPingServerClient(conn)
		res, err := pingClient.Ping(ctx, &emptypb.Empty{})
		if err != nil {
			color.Red("ping controller: %s failed", endpoints[idx])
			continue
		}
		leaderAddr = res.LeaderAddr
		break
	}
	conn, exist := connMap[leaderAddr]
	if exist {
		if conn == nil {
			cmdFailedf("connect to leader:%s failed", leaderAddr)
		}
		return conn
	}
	conn = createGRPCConn(ctx, leaderAddr)
	if conn == nil {
		cmdFailedf("connect to leader:%s failed", leaderAddr)
	}
	return conn
}

func createGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	var err error
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())
	ctx, cancel := context.WithCancel(ctx)
	timeout := false
	go func() {
		ticker := time.Tick(time.Second)
		select {
		case <-ctx.Done():
		case <-ticker:
			cancel()
			timeout = true
		}
	}()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	cancel()
	if timeout {
		color.Yellow("dial to controller: %s timeout, try to another controller", addr)
		return nil
	} else if err != nil {
		color.Red("dial to controller: %s failed", addr)
		return nil
	}
	return conn
}
