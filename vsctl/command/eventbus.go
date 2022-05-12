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

	"github.com/fatih/color"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/spf13/cobra"
)

func NewEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "eventbus sub-command",
		Short: "sub-commands for eventbus operations",
	}
	cmd.AddCommand(createEventbusCommand())
	cmd.AddCommand(deleteEventbusCommand())
	return cmd
}

func createEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <eventbus-name> ",
		Short: "create a eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				color.White("eventbus name can't be empty\n")
				color.Cyan("\n============ see below for right usage ============\n\n")
				_ = cmd.Help()
				os.Exit(-1)
			}
			ctx := context.Background()
			grpcConn := mustGetGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			_, err := cli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: args[0],
			})
			if err != nil {
				cmdFailedf("create eventbus failed: %s", err)
			}
			color.Green("create eventbus: %s success\n", args[0])
		},
	}
	cmd.Flags().String("name", "", "eventbus name to creating")
	return cmd
}

func deleteEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <eventbus-name> ",
		Short: "delete a eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				color.White("eventbus name can't be empty\n")
				color.Cyan("\n============ see below for right usage ============\n\n")
				_ = cmd.Help()
				os.Exit(-1)
			}
			ctx := context.Background()
			grpcConn := mustGetGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			_, err := cli.DeleteEventBus(ctx, &metapb.EventBus{Name: args[0]})
			if err != nil {
				cmdFailedf("delete eventbus failed: %s", err)
			}
			color.Green("delete eventbus: %s success\n", args[0])
		},
	}
	cmd.Flags().String("name", "", "eventbus name to deleting")
	return cmd
}
