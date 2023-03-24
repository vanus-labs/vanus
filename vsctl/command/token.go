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
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	"github.com/vanus-labs/vanus/proto/pkg/meta"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func NewTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token sub-command",
		Short: "sub-commands for token operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(createTokenCommand())
	cmd.AddCommand(deleteTokenCommand())
	cmd.AddCommand(getTokenCommand())
	cmd.AddCommand(listTokenInfoCommand())
	return cmd
}

func createTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a user token",
		Run: func(cmd *cobra.Command, args []string) {
			token, err := client.CreateToken(context.Background(), &ctrlpb.CreateTokenRequest{
				UserIdentifier: userIdentifier,
			})
			if err != nil {
				cmdFailedf(cmd, "create user token failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Success", "user": token.UserIdentifier, "token": token.Token})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendRow(table.Row{"User ", token.UserIdentifier})
				t.AppendRow(table.Row{"Token ", token.Token})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user token to creating, default current user")
	return cmd
}

func deleteTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a user token",
		Run: func(cmd *cobra.Command, args []string) {
			id, err := vanus.NewIDFromString(idStr)
			if err != nil {
				cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid token id: %s\n", err.Error()))
			}
			_, err = client.DeleteToken(context.Background(),
				&ctrlpb.DeleteTokenRequest{Id: id.Uint64()})
			if err != nil {
				cmdFailedf(cmd, "delete user token failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Success", "ID": idStr})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "ID"})
				t.AppendRow(table.Row{"Delete Success", idStr})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&idStr, "id", "", "user token to deleting")
	return cmd
}

func getTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get the user token",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			res, err := client.GetUserToken(ctx, wrapperspb.String(userIdentifier))
			if err != nil {
				cmdFailedf(cmd, "get user failed: %s", err)
			}
			printToken(cmd, true, res.GetToken()...)
		},
	}
	cmd.Flags().StringVar(&userIdentifier, "user", "", "user token to show, default current user")
	return cmd
}

func listTokenInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list all user token",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := client.ListToken(context.Background(), &emptypb.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list user token failed: %s", err)
			}
			printToken(cmd, false, res.GetToken()...)
		},
	}
	return cmd
}

func printToken(cmd *cobra.Command, showNo bool, data ...*metapb.Token) {
	if IsFormatJSON(cmd) {
		data, _ := json.Marshal(data)
		color.Green(string(data))
	} else {
		t := table.NewWriter()
		header := getTokenHeader(showNo)
		t.AppendHeader(header)
		for idx := range data {
			var row []interface{}
			if showNo {
				row = append(row, idx+1)
			}
			ns := data[idx]

			row = append(row, getTokenRow(ns)...)
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
func getTokenHeader(showNo bool) table.Row {
	var result []interface{}
	if showNo {
		result = append(result, "no.")
	}
	result = append(result, "id", "userIdentifier", "token", "created_at", "updated_at")
	return result
}

func getTokenRow(token *meta.Token) []interface{} {
	var result []interface{}
	result = append(result, formatID(token.Id))
	result = append(result, token.UserIdentifier)
	result = append(result, token.Token)
	return result
}
