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
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/spf13/cobra"
)

func NewSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription sub-command ",
		Short: "sub-commands for subscription operations",
	}
	cmd.AddCommand(createSubscriptionCommand())
	cmd.AddCommand(deleteSubscriptionCommand())
	return cmd
}

func createSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <subscription-name> ",
		Short: "create a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				color.White("eventbus name can't be empty\n")
				color.Cyan("\n============ see below for right usage ============\n\n")
				_ = cmd.Help()
				os.Exit(-1)
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()
			filterObj := make(map[string]interface{}, 0)
			err := json.Unmarshal([]byte(filters), filterObj)
			if err != nil {
				cmdFailedf("the filter invalid: %s", err)
			}
			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			_, err = cli.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
				Source:   source,
				Filters:  nil,
				Sink:     sink,
				EventBus: eventbus,
			})
			if err != nil {
				cmdFailedf("create subscription failed: %s", err)
			}
			color.Green("create subscription: %s success\n", args[0])
		},
	}
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus name to consuming")
	cmd.Flags().StringVar(&source, "source", "", "the event from which source")
	cmd.Flags().StringVar(&sink, "sink", "", "the event you want to send to")
	cmd.Flags().StringVar(&filters, "filters", "", "filter event you interested, JSON format required")
	return cmd
}

func deleteSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <subscription-name> ",
		Short: "delete a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				color.White("eventbus name can't be empty\n")
				color.Cyan("\n============ see below for right usage ============\n\n")
				_ = cmd.Help()
				os.Exit(-1)
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
			color.Green("delete subscription: %s success\n", args[0])
		},
	}
	cmd.Flags().Uint64Var(&subscriptionID, "id", 0, "subscription id to deleting")
	return cmd
}
