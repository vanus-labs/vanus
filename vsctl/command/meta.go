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
	"os"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/spf13/cobra"
)

func NewClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster sub-command ",
		Short: "vanus cluster operations",
	}
	cmd.AddCommand(controllerCommand())
	return cmd
}

func controllerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "controller sub-command",
		Short: "get controller metadata",
	}
	cmd.AddCommand(getControllerTopology())
	return cmd
}

func getControllerTopology() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topology",
		Short: "get topology",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()
			cli := ctrlpb.NewPingServerClient(grpcConn)
			res, err := cli.Ping(ctx, &empty.Empty{})
			if err != nil {
				cmdFailedf(cmd, "get Gateway endpoint from controller failed: %s", err)
			}

			t := table.NewWriter()
			t.AppendHeader(table.Row{"Name", "Leader", "Endpoint"})
			t.AppendRows([]table.Row{
				{"Leader-controller", "TRUE", res.LeaderAddr},
				{"Gateway", "-", res.GatewayAddr},
			})
			t.SetColumnConfigs([]table.ColumnConfig{
				{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				{Number: 3, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
			})
			t.SetStyle(table.StyleLight)
			t.Style().Options.SeparateRows = true
			t.Style().Box = table.StyleBoxDefault
			t.SetOutputMirror(os.Stdout)
			t.Render()
		},
	}
	return cmd
}
