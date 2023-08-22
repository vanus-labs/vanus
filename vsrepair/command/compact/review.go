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

package compact

import (
	// standard libraries.
	"encoding/json"
	"fmt"
	"strconv"

	// third-party libraries.
	"github.com/spf13/cobra"

	// this project.
	"github.com/vanus-labs/vanus/vsrepair/meta"
)

func ReviewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compact ID",
		Short: "Review compact history of a specific Block",
		Run:   review,
	}
	cmd.Flags().StringVar(&volumePath, "volume", "", "volume path")
	return cmd
}

type reviewResult struct {
	Version int64             `json:"version"`
	Compact *meta.CompactInfo `json:"Compact"`
}

func review(cmd *cobra.Command, args []string) {
	for _, arg := range args {
		id, err := strconv.ParseUint(arg, 0, 64)
		if err != nil {
			panic(err)
		}

		err = meta.ReviewCompact(volumePath, id, func(compact *meta.CompactInfo, version int64) {
			jsonResult, err2 := json.MarshalIndent(reviewResult{
				Version: version,
				Compact: compact,
			}, "", "  ")
			if err2 != nil {
				panic(err2)
			}
			fmt.Println(string(jsonResult))
		})
		if err != nil {
			panic(err)
		}
	}
}
