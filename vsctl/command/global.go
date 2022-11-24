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
	"strings"
	"time"

	"github.com/fatih/color"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	FormatJSON = "json"
)

type GlobalFlags struct {
	Endpoint   string
	Debug      bool
	ConfigFile string
	Format     string
}

var (
	client proxypb.ControllerProxyClient
	cc     *grpc.ClientConn
)

func InitGatewayClient(cmd *cobra.Command) {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		panic("failed to dial gateway: " + err.Error())
	}
	cc = conn
	client = proxypb.NewControllerProxyClient(conn)
}

func DestroyGatewayClient() {
	if cc != nil {
		if err := cc.Close(); err != nil {
			color.Yellow(fmt.Sprintf("close grpc connection error: %s", err.Error()))
		}
	}
}

func mustGetGatewayCloudEventsEndpoint(cmd *cobra.Command) string {
	res, err := client.ClusterInfo(context.Background(), &emptypb.Empty{})
	if err != nil {
		cmdFailedf(cmd, "get cloudevents endpoint failed: %s", err)
	}
	sp := strings.Split(mustGetGatewayEndpoint(cmd), ":")
	return fmt.Sprintf("%s:%d", sp[0], res.CloudeventsPort)
}

func mustGetGatewayEndpoint(cmd *cobra.Command) string {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		cmdFailedf(cmd, "get gateway endpoint failed: %s", err)
	}
	return endpoint
}

func IsFormatJSON(cmd *cobra.Command) bool {
	v, err := cmd.Flags().GetString("format")
	if err != nil {
		return false
	}
	return strings.ToLower(v) == FormatJSON
}
