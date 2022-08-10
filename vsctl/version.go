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

package main

import (
	"encoding/json"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/linkall-labs/vanus/vsctl/command"
	"github.com/spf13/cobra"
	"os"
)

var (
	Version   string
	GitCommit string
	BuildDate string
	GoVersion string
	Platform  string
)

func newVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "get vsctl version info",
		Run: func(cmd *cobra.Command, args []string) {
			if !command.IsFormatJSON(cmd) {
				t := table.NewWriter()
				t.AppendRow(table.Row{"Version", Version})
				t.AppendRow(table.Row{"Platform", Platform})
				t.AppendRow(table.Row{"GitCommit", GitCommit})
				t.AppendRow(table.Row{"BuildDate", BuildDate})
				t.AppendRow(table.Row{"GoVersion", GoVersion})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter},
					{Number: 2, Align: text.AlignLeft},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			} else {
				info := map[string]string{
					"Version":   Version,
					"Platform":  Platform,
					"GitCommit": GitCommit,
					"BuildDate": BuildDate,
					"GoVersion": GoVersion,
				}
				data, _ := json.MarshalIndent(info, "", "  ")
				color.Green(string(data))
			}
		},
	}
	return cmd
}
