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
	"time"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func NewUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user sub-command",
		Short: "sub-commands for user operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(createUserCommand())
	cmd.AddCommand(deleteUserCommand())
	cmd.AddCommand(getUserInfoCommand())
	cmd.AddCommand(listUserInfoCommand())
	cmd.AddCommand(getUserRolesCommand())
	cmd.AddCommand(NewTokenCommand())
	return cmd
}

func createUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a user",
		Run: func(cmd *cobra.Command, args []string) {
			if userIdentifier == "" {
				cmdFailedf(cmd, "the --user flag MUST be set")
			}
			_, err := client.CreateUser(context.Background(), &ctrlpb.CreateUserRequest{
				Identifier:  userIdentifier,
				Description: description,
			})
			if err != nil {
				cmdFailedf(cmd, "create user failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Success", "user": userIdentifier})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "user"})
				t.AppendRow(table.Row{"Success to create user ", userIdentifier})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user to creating")
	cmd.Flags().StringVar(&description, "description", "", "user description")
	return cmd
}

func deleteUserCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a user",
		Run: func(cmd *cobra.Command, args []string) {
			if userIdentifier == "" {
				cmdFailedf(cmd, "the --user flag MUST be set")
			}

			_, err := client.DeleteUser(context.Background(), wrapperspb.String(userIdentifier))
			if err != nil {
				cmdFailedf(cmd, "delete user failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Success", "user": userIdentifier})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "user"})
				t.AppendRow(table.Row{"Delete Success", userIdentifier})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user to deleting")
	return cmd
}

func getUserInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <> ",
		Short: "get the user info",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			res, err := client.GetUser(ctx, wrapperspb.String(userIdentifier))
			if err != nil {
				cmdFailedf(cmd, "get user failed: %s", err)
			}
			printUser(cmd, false, res)
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user to show, default current user")
	return cmd
}

func listUserInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the user",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := client.ListUser(context.Background(), &emptypb.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list user failed: %s", err)
			}
			printUser(cmd, false, res.GetUsers()...)
		},
	}
	return cmd
}

func getUserRolesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "roles",
		Short: "get the user roles",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := client.GetUserRole(context.Background(),
				&ctrlpb.GetUserRoleRequest{UserIdentifier: userIdentifier})
			if err != nil {
				cmdFailedf(cmd, "list user role failed: %s", err)
			}
			printUserRoles(cmd, true, res.GetUserRole()...)
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user roles to show, default current user")
	return cmd
}

func printUser(cmd *cobra.Command, showNo bool, data ...*metapb.User) {
	if IsFormatJSON(cmd) {
		data, _ := json.Marshal(data)
		color.Green(string(data))
	} else {
		t := table.NewWriter()
		header := getUserHeader(showNo)
		t.AppendHeader(header)
		for idx := range data {
			var row []interface{}
			if showNo {
				row = append(row, idx+1)
			}
			ns := data[idx]

			row = append(row, getUserRow(ns)...)
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

func getUserHeader(showNo bool) table.Row {
	var result []interface{}
	if showNo {
		result = append(result, "no.")
	}
	result = append(result, "userIdentifier", "description", "created_at", "updated_at")
	return result
}

func getUserRow(user *meta.User) []interface{} {
	var result []interface{}
	result = append(result, user.Identifier)
	result = append(result, user.Description)
	return result
}

func printUserRoles(cmd *cobra.Command, showNo bool, data ...*metapb.UserRole) {
	if IsFormatJSON(cmd) {
		data, _ := json.Marshal(data)
		color.Green(string(data))
	} else {
		t := table.NewWriter()
		header := getUserRoleHeader(showNo)
		t.AppendHeader(header)
		for idx := range data {
			var row []interface{}
			if showNo {
				row = append(row, idx+1)
			}
			ns := data[idx]

			row = append(row, getUserRoleRow(ns)...)
			row = append(row, time.UnixMilli(ns.CreatedAt).Format(time.RFC3339))
			t.AppendRow(row)
			t.AppendSeparator()
		}
		t.SetColumnConfigs(getColumnConfig(header))
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}
}

func getUserRoleHeader(showNo bool) table.Row {
	var result []interface{}
	if showNo {
		result = append(result, "no.")
	}
	result = append(result, "userIdentifier", "role", "resourceKind", "resourceID", "created_at")
	return result
}

func getUserRoleRow(user *meta.UserRole) []interface{} {
	var result []interface{}
	result = append(result, user.UserIdentifier)
	result = append(result, user.RoleName)
	result = append(result, user.ResourceKind)
	result = append(result, formatID(user.ResourceId))
	return result
}
