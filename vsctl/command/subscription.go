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
	"bytes"
	"context"
	"encoding/json"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/linkall-labs/vsproto/pkg/meta"
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
					cmdFailedf("the filter invalid: %s", err)
				}
			}

			var inputTrans *meta.InputTransformer
			if inputTransformer != "" {
				err := json.Unmarshal([]byte(inputTransformer), &inputTrans)
				if err != nil {
					cmdFailedf("the inputTransformer invalid: %s", err)
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
				cmdFailedf("create subscription failed: %s", err)
			}

			color.Green("create subscription: %d success\n", res.Id)
		},
	}
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus name to consuming")
	cmd.Flags().StringVar(&source, "source", "", "the event from which source")
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
				cmdFailedf("delete subscription failed: %s", err)
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
				cmdFailedf("get subscription info failed: %s", err)
			}
			data, _ := json.Marshal(res)
			var out bytes.Buffer
			_ = json.Indent(&out, data, "", "\t")
			color.Green("%s", out.String())
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
				cmdFailedf("list subscription failed: %s", err)
			}
			data, _ := json.Marshal(res)
			var out bytes.Buffer
			_ = json.Indent(&out, data, "", "\t")
			color.Green("%s", out.String())
		},
	}
	return cmd
}
