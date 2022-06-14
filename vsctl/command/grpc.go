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
	"strings"
	"time"

	"github.com/fatih/color"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func mustGetLeaderControllerGRPCConn(ctx context.Context,
	cmd *cobra.Command) *grpc.ClientConn {
	endpoints := mustEndpointsFromCmd(cmd)
	var leaderAddr string
	var leaderConn *grpc.ClientConn
	tryConnectLeaderOnce := false
	for idx := range endpoints {
		conn := createGRPCConn(ctx, endpoints[idx])
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
		if leaderAddr == endpoints[idx] {
			leaderConn = conn
			tryConnectLeaderOnce = false
		} else {
			_ = conn.Close()
		}
		break
	}

	if leaderAddr == "" {
		cmdFailedf(cmd, "the leader controller not found")
	}

	if leaderConn != nil {
		return leaderConn
	} else if !tryConnectLeaderOnce {
		leaderConn = createGRPCConn(ctx, mappingLeaderAddr(leaderAddr))
	}

	if leaderConn == nil {
		cmdFailedf(cmd, "connect to leader: %s failed", leaderAddr)
	}
	return leaderConn
}

func createGRPCConn(ctx context.Context, addr string) *grpc.ClientConn {
	if addr == "" {
		return nil
	}
	addr = strings.TrimPrefix(addr, "http://")
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

func mappingLeaderAddr(addr string) string {
	m := map[string]string{
		"vanus-controller-0.vanus-controller.vanus.svc:2048": "192.168.49.2:32000",
		"vanus-controller-1.vanus-controller.vanus.svc:2048": "192.168.49.2:32100",
		"vanus-controller-2.vanus-controller.vanus.svc:2048": "192.168.49.2:32200"}
	return m[addr]
}
