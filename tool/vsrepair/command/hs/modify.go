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

package hs

import (
	// standard libraries.
	"strconv"

	// third-party libraries.
	"github.com/spf13/cobra"

	// this project.
	"github.com/vanus-labs/vanus/tool/vsrepair/meta"
)

func ModifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hs [--volume <volume path>] nodeID [--commit offset]",
		Short: "modify HardState",
		Run:   modify,
	}
	cmd.Flags().Uint64Var(&commit, "commit", 0, "commit offset")
	return cmd
}

func modify(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		panic("invalid args")
	}

	db, err := meta.Open(volumePath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	node, err := strconv.ParseUint(args[0], 0, 0)
	if err != nil {
		panic(err)
	}

	hs, err := db.GetHardState(node)
	if err != nil {
		panic(err)
	}

	if commit != 0 {
		hs.Commit = commit
	}

	if err := db.PutHardState(node, hs); err != nil {
		panic(err)
	}
}
