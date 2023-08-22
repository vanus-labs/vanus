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

package apply

import (
	// standard libraries.
	"strconv"

	// third-party libraries.
	"github.com/spf13/cobra"

	// this project.
	"github.com/vanus-labs/vanus/vsrepair/meta"
)

func ModifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "apply [--volume <volume path>] nodeID offset",
		Short: "modify apply offset",
		Run:   modify,
	}
	cmd.Flags().StringVar(&volumePath, "volume", "", "volume path")
	return cmd
}

func modify(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
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

	app, err := strconv.ParseUint(args[1], 10, 0)
	if err != nil {
		panic(err)
	}

	if err := db.PutApply(node, app); err != nil {
		panic(err)
	}
}
