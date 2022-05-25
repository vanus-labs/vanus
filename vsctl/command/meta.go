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
	"github.com/spf13/cobra"
)

func NewMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event sub-command ",
		Short: "convenient operations for pub/sub",
	}
	cmd.AddCommand(controllerCommand())
	return cmd
}

func controllerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "controller <eventbus-name> ",
		Short: "get controller metadata",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
			}
			//endpoint := mustGetGatewayEndpoint(cmd)

		},
	}
	return cmd
}
