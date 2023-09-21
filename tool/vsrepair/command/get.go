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
	// third-party libraries.
	"github.com/spf13/cobra"

	// this project.
	"github.com/vanus-labs/vanus/tool/vsrepair/command/apply"
	"github.com/vanus-labs/vanus/tool/vsrepair/command/compact"
	"github.com/vanus-labs/vanus/tool/vsrepair/command/cs"
	"github.com/vanus-labs/vanus/tool/vsrepair/command/event"
	"github.com/vanus-labs/vanus/tool/vsrepair/command/hs"
)

func GetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get SUB-COMMAND",
		Short: "get metadata",
	}
	cmd.AddCommand(cs.GetCommand())
	cmd.AddCommand(hs.GetCommand())
	cmd.AddCommand(apply.GetCommand())
	cmd.AddCommand(compact.GetCommand())
	cmd.AddCommand(event.GetCommand())
	return cmd
}
