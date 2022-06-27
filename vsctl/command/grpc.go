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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func mustGetControllerProxyConn(ctx context.Context, cmd *cobra.Command) *grpc.ClientConn {
	splits := strings.Split(mustGetGatewayEndpoint(cmd), ":")
	port, err := strconv.Atoi(splits[1])
	if err != nil {
		cmdFailedf(cmd, "parsing gateway port failed")
	}
	leaderConn := createGRPCConn(ctx, fmt.Sprintf("%s:%d", splits[0], port+2))
	if leaderConn == nil {
		cmdFailedf(cmd, "failed to connect to gateway")
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
