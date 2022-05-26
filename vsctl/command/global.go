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
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"strings"

	"github.com/spf13/cobra"
)

type GlobalFlags struct {
	Endpoints  []string
	Debug      bool
	ConfigFile string
}

func mustEndpointsFromCmd(cmd *cobra.Command) []string {
	eps, err := cmd.Flags().GetStringSlice("endpoints")
	if err != nil {
		cmdFailedf("get controller endpoints failed: %s", err)
	}
	if err == nil {
		for i, ip := range eps {
			eps[i] = strings.TrimSpace(ip)
		}
	}
	return eps
}

var styleVanus = table.Style{
	Name:    "StyleVanus",
	Box:     table.StyleBoxDefault,
	Color:   table.ColorOptionsDefault,
	Format:  table.FormatOptionsDefault,
	HTML:    table.DefaultHTMLOptions,
	Options: table.OptionsDefault,
	Title: table.TitleOptions{
		Align: text.AlignCenter,
	},
}
