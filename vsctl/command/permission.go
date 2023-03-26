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

package command

import (
	"context"
	"encoding/json"
	"os"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
)

func NewPermissionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "permission sub-command",
		Short: "sub-commands permission for operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(grantPermissionCommand())
	cmd.AddCommand(revokePermissionCommand())
	return cmd
}

func grantPermissionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "grant",
		Short: "grant a permission",
		Run: func(cmd *cobra.Command, args []string) {
			req := makePermissionRequest(cmd)
			_, err := client.GrantRole(context.Background(), req)
			if err != nil {
				cmdFailedf(cmd, "permission grant failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Grant Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"result", "user", "role"})
				t.AppendRow(table.Row{"grant success", userIdentifier, role})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
					{Number: 3, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user identifier")
	cmd.Flags().StringVar(&role, "role", "", "role name")
	return cmd
}

func revokePermissionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke",
		Short: "revoke a permission",
		Run: func(cmd *cobra.Command, args []string) {
			req := makePermissionRequest(cmd)
			_, err := client.RevokeRole(context.Background(), req)
			if err != nil {
				cmdFailedf(cmd, "permission revoke failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Revoke Success"})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"result", "user", "role"})
				t.AppendRow(table.Row{"revoke success", userIdentifier, role})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
					{Number: 3, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user identifier")
	cmd.Flags().StringVar(&role, "role", "", "role name")
	return cmd
}

func makePermissionRequest(cmd *cobra.Command) *ctrlpb.RoleRequest {
	if userIdentifier == "" {
		cmdFailedf(cmd, "the --user flag MUST be set")
	}
	if role == "" {
		cmdFailedf(cmd, "the --role flag MUST be set")
	}
	if role == "clusterAdmin" {
		cmdFailedf(cmd, "the role now only support clusterAdmin")
	}
	return &ctrlpb.RoleRequest{
		UserIdentifier: userIdentifier,
		RoleName:       role,
	}
}
