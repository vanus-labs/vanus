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
	"encoding/json"
	"os"

	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	"github.com/spf13/cobra"
)

func NewSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription sub-command ",
		Short: "sub-commands for subscription operations",
	}
	cmd.AddCommand(createSubscriptionCommand())
	cmd.AddCommand(deleteSubscriptionCommand())
	cmd.AddCommand(getSubscriptionCommand())
	cmd.AddCommand(listSubscriptionCommand())
	return cmd
}

func createSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if eventbus == "" {
				cmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
			}
			if sink == "" {
				cmdFailedWithHelpNotice(cmd, "sink name can't be empty\n")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()
			var filter []*meta.Filter
			if filters != "" {
				err := json.Unmarshal([]byte(filters), &filter)
				if err != nil {
					cmdFailedf(cmd, "the filter invalid: %s", err)
				}
			}

			var inputTrans *meta.InputTransformer
			if inputTransformer != "" {
				err := json.Unmarshal([]byte(inputTransformer), &inputTrans)
				if err != nil {
					cmdFailedf(cmd, "the inputTransformer invalid: %s", err)
				}
			}

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
				Source:           source,
				Filters:          filter,
				Sink:             sink,
				EventBus:         eventbus,
				InputTransformer: inputTrans,
			})
			if err != nil {
				cmdFailedf(cmd, "create subscription failed: %s", err)
			}
			if isOutputFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{
					"id":          res.Id,
					"eventbus":    eventbus,
					"filter":      filter,
					"sink":        sink,
					"transformer": inputTransformer,
				})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"id", "eventbus", "sink", "filter", "transformer"})
				data1, _ := json.MarshalIndent(filter, "", "  ")
				data2, _ := json.MarshalIndent(inputTransformer, "", "  ")
				t.AppendRow(table.Row{res.Id, eventbus, sink, string(data1), string(data2)})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 4, AlignHeader: text.AlignCenter},
					{Number: 5, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus name to consuming")
	cmd.Flags().StringVar(&sink, "sink", "", "the event you want to send to")
	cmd.Flags().StringVar(&filters, "filters", "", "filter event you interested, JSON format required")
	cmd.Flags().StringVar(&inputTransformer, "input-transformer", "", "input transformer, JSON format required")
	return cmd
}

func deleteSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if subscriptionID == 0 {
				cmdFailedWithHelpNotice(cmd, "subscriptionID name can't be empty\n")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			_, err := cli.DeleteSubscription(ctx, &ctrlpb.DeleteSubscriptionRequest{
				Id: subscriptionID,
			})
			if err != nil {
				cmdFailedf(cmd, "delete subscription failed: %s", err)
			}

			if isOutputFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"subscription_id": subscriptionID})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"subscriptionID"})
				t.AppendRow(table.Row{subscriptionID})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
			color.Green("delete subscription: %d success\n", subscriptionID)
		},
	}
	cmd.Flags().Uint64Var(&subscriptionID, "id", 0, "subscription id to deleting")
	return cmd
}

func getSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "get the subscription info ",
		Run: func(cmd *cobra.Command, args []string) {
			if subscriptionID == 0 {
				cmdFailedWithHelpNotice(cmd, "subscriptionID name can't be empty\n")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{
				Id: subscriptionID,
			})
			if err != nil {
				cmdFailedf(cmd, "get subscription info failed: %s", err)
			}
			if isOutputFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"id", "eventbus", "sink", "filter", "transformer"})
				data1, _ := json.MarshalIndent(res.Filters, "", "  ")
				data2, _ := json.MarshalIndent(res.InputTransformer, "", "  ")

				t.AppendRow(table.Row{res.Id, res.EventBus, res.Sink, string(data1), string(data2)})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 4, AlignHeader: text.AlignCenter},
					{Number: 5, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().Uint64Var(&subscriptionID, "id", 0, "subscription id to deleting")
	return cmd
}

func listSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the subscription ",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.ListSubscription(ctx, &empty.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list subscription failed: %s", err)
			}
			if isOutputFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"no.", "id", "eventbus", "sink", "filter", "transformer"})
				for idx := range res.Subscription {
					sub := res.Subscription[idx]
					data1, _ := json.MarshalIndent(sub.Filters, "", "  ")
					data2, _ := json.MarshalIndent(sub.InputTransformer, "", "  ")
					t.AppendRow(table.Row{idx + 1, sub.Id, sub.EventBus, sub.Sink, string(data1),
						string(data2)})
					t.AppendSeparator()
				}
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 3, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 4, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 5, AlignHeader: text.AlignCenter},
					{Number: 6, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}
