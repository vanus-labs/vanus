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

import "github.com/spf13/cobra"

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
			println("subscription create")
		},
	}
	cmd.Flags().String("filter", "{}", "")
	cmd.Flags().String("eventbus", "", "eventbus name to consuming")
	return cmd
}

func deleteSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <subscription-name> ",
		Short: "delete a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			println("subscription delete")
		},
	}
	cmd.Flags().String("id", "", "subscription id to deleting")
	return cmd
}
