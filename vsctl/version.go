package main

import (
	"encoding/json"
	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/linkall-labs/vanus/vsctl/command"
	"github.com/spf13/cobra"
	"os"
	"runtime"
)

var (
	Version   string
	GitCommit string
	BuildDate string
	GoVersion = runtime.Version()
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
