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
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func NewNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "namespace sub-command",
		Short: "sub-commands for namespace operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(createNamespaceCommand())
	cmd.AddCommand(deleteNamespaceCommand())
	cmd.AddCommand(getNamespaceInfoCommand())
	cmd.AddCommand(listNamespaceInfoCommand())
	cmd.AddCommand(grantNamespaceCommand())
	cmd.AddCommand(revokeNamespaceCommand())
	return cmd
}

func createNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a namespace",
		Run: func(cmd *cobra.Command, args []string) {
			if namespace == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			var id uint64
			if idStr != "" {
				vID, err := vanus.NewIDFromString(idStr)
				if err != nil {
					cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid namespace id: %s\n", err.Error()))
				}
				id = vID.Uint64()
			}
			_, err := client.CreateNamespace(context.Background(), &ctrlpb.CreateNamespaceRequest{
				Id:          id,
				Name:        namespace,
				Description: description,
			})
			if err != nil {
				cmdFailedf(cmd, "create namespace failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Success", "namespace": namespace})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "namespace"})
				t.AppendRow(table.Row{"Success to create namespace ", namespace})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&idStr, "id", "", "namespace id to create")
	cmd.Flags().StringVar(&namespace, "name", "", "namespace name to creating")
	cmd.Flags().StringVar(&description, "description", "", "namespace description")
	return cmd
}

func deleteNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <namespace-name> ",
		Short: "delete a namespace",
		Run: func(cmd *cobra.Command, args []string) {
			if namespace == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}

			_, err := client.DeleteNamespace(context.Background(),
				&ctrlpb.DeleteNamespaceRequest{Id: mustGetNamespaceID(namespace).Uint64()})
			if err != nil {
				cmdFailedf(cmd, "delete namespace failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"namespace": namespace})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "namespace"})
				t.AppendRow(table.Row{"Delete Success", namespace})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&namespace, "name", "", "namespace name to deleting")
	return cmd
}

func getNamespaceInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info [flag] ",
		Short: "get the namespace info",
		Run: func(cmd *cobra.Command, args []string) {
			if namespace == "" {
				cmdFailedf(cmd, "the namespace must be set")
			}

			ctx := context.Background()
			res, err := client.GetNamespace(ctx,
				&ctrlpb.GetNamespaceRequest{Id: mustGetNamespaceID(namespace).Uint64()})
			if err != nil {
				cmdFailedf(cmd, "get namespace failed: %s", err)
			}
			printNamespace(cmd, false, res)
		},
	}
	cmd.Flags().StringVar(&namespace, "name", "", "namespace to show, use , to separate")
	return cmd
}

func listNamespaceInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the namespace",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := client.ListNamespace(context.Background(), &emptypb.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list namespace failed: %s", err)
			}
			printNamespace(cmd, false, res.GetNamespace()...)
		},
	}
	return cmd
}

func grantNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "grant",
		Short: "grant a namespace",
		Run: func(cmd *cobra.Command, args []string) {
			if namespace == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			if userIdentifier == "" {
				cmdFailedf(cmd, "the --user flag MUST be set")
			}
			if role == "" {
				cmdFailedf(cmd, "the --role flag MUST be set")
			}
			_, err := client.GrantRole(context.Background(), &ctrlpb.RoleRequest{
				UserIdentifier: userIdentifier,
				RoleName:       role,
				ResourceKind:   "namespace",
				ResourceId:     mustGetNamespaceID(namespace).Uint64(),
			})
			if err != nil {
				cmdFailedf(cmd, "permission grant failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Grant Success", "Namespace": namespace})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"result", "user", "namespace", "role"})
				t.AppendRow(table.Row{"Grant Success", userIdentifier, namespace, role})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
					{Number: 3, AlignHeader: text.AlignCenter},
					{Number: 4, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user identifier")
	cmd.Flags().StringVar(&role, "role", "", "role name")
	cmd.Flags().StringVar(&namespace, "name", "", "namespace to grant")
	return cmd
}

func revokeNamespaceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke",
		Short: "revoke a namespace",
		Run: func(cmd *cobra.Command, args []string) {
			if namespace == "" {
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			if userIdentifier == "" {
				cmdFailedf(cmd, "the --user flag MUST be set")
			}
			if role == "" {
				cmdFailedf(cmd, "the --role flag MUST be set")
			}
			_, err := client.RevokeRole(context.Background(), &ctrlpb.RoleRequest{
				UserIdentifier: userIdentifier,
				RoleName:       role,
				ResourceKind:   "namespace",
				ResourceId:     mustGetNamespaceID(namespace).Uint64(),
			})
			if err != nil {
				cmdFailedf(cmd, "permission revoke failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Revoke Success", "Namespace": namespace})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"result", "user", "namespace", "role"})
				t.AppendRow(table.Row{"Revoke Success", userIdentifier, namespace, role})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
					{Number: 3, AlignHeader: text.AlignCenter},
					{Number: 4, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user identifier")
	cmd.Flags().StringVar(&role, "role", "", "role name")
	cmd.Flags().StringVar(&namespace, "name", "", "namespace to grant")
	return cmd
}

func printNamespace(cmd *cobra.Command, showNo bool, data ...*metapb.Namespace) {
	if IsFormatJSON(cmd) {
		data, _ := json.Marshal(data)
		color.Green(string(data))
	} else {
		t := table.NewWriter()
		header := getNamespaceHeader(showNo)
		t.AppendHeader(header)
		for idx := range data {
			var row []interface{}
			if showNo {
				row = append(row, idx+1)
			}
			ns := data[idx]

			row = append(row, getNamespaceRow(ns)...)
			row = append(row, time.UnixMilli(ns.CreatedAt).Format(time.RFC3339))
			row = append(row, time.UnixMilli(ns.UpdatedAt).Format(time.RFC3339))
			t.AppendRow(row)
			t.AppendSeparator()
		}
		t.SetColumnConfigs(getColumnConfig(header))
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}
}

func getNamespaceHeader(showNo bool) table.Row {
	var result []interface{}
	if showNo {
		result = append(result, "no.")
	}
	result = append(result, "id", "name", "description", "created_at", "updated_at")
	return result
}

func getNamespaceRow(ns *meta.Namespace) []interface{} {
	var result []interface{}
	result = append(result, formatID(ns.Id))
	result = append(result, ns.Name)
	result = append(result, ns.Description)
	return result
}
