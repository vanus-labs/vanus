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
	"bytes"
	"context"
	"encoding/json"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	"github.com/spf13/cobra"
	"os"
)

func NewEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "eventbus sub-command",
		Short: "sub-commands for eventbus operations",
	}
	cmd.AddCommand(createEventbusCommand())
	cmd.AddCommand(deleteEventbusCommand())
	cmd.AddCommand(getEventbusInfoCommand())
	cmd.AddCommand(listEventbusInfoCommand())
	return cmd
}

func createEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if eventbus == "" {
				cmdFailedf("the --name flag MUST> be set")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			_, err := cli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: eventbus,
			})
			if err != nil {
				cmdFailedf("create eventbus failed: %s", err)
			}
			color.Green("create eventbus: %s success\n", eventbus)
		},
	}
	cmd.Flags().StringVar(&eventbus, "name", "", "eventbus name to deleting")
	return cmd
}

func deleteEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <eventbus-name> ",
		Short: "delete a eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if eventbus == "" {
				cmdFailedf("the --name flag MUST> be set")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
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
	cmd.Flags().StringVar(&eventbus, "name", "", "eventbus name to deleting")
	return cmd
}

func getEventbusInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info <eventbus-name> ",
		Short: "get the eventbus info",
		Run: func(cmd *cobra.Command, args []string) {
			if args[0] == "" {
				cmdFailedf("the eventbus must be set")
			}
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			res, err := cli.GetEventBus(ctx, &metapb.EventBus{Name: args[0]})
			if err != nil {
				cmdFailedf("get eventbus failed: %s", err)
			}

			segs := make(map[uint64][]*metapb.Segment)
			if showSegment {
				logCli := ctrlpb.NewEventLogControllerClient(grpcConn)
				logs := res.GetLogs()
				for idx := range logs {
					segRes, err := logCli.ListSegment(ctx, &ctrlpb.ListSegmentRequest{
						EventBusId: res.Id,
						EventLogId: logs[idx].EventLogId,
					})
					if err != nil {
						cmdFailedf("get segments failed: %s", err)
					}
					segs[logs[idx].EventLogId] = segRes.Segments
				}
			}

			t := table.NewWriter()

			if !showSegment && !showBlock {
				t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment Number"})
				for idx := 0; idx < len(res.Logs); idx++ {
					if idx == 0 {
						t.AppendRow(table.Row{res.Name, res.Logs[idx].EventLogId, res.Logs[idx].CurrentSegmentNumbers})
					} else {
						t.AppendRow(table.Row{"", res.Logs[idx].EventLogId, res.Logs[idx].CurrentSegmentNumbers})
					}
				}
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 3, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
			} else {
				if !showBlock {
					t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment", "Start", "End"})
					for idx := 0; idx < len(res.Logs); idx++ {
						segOfEL := segs[res.Logs[idx].EventLogId]
						for sIdx, v := range segOfEL {
							if idx == 0 && sIdx == 0 {
								t.AppendRow(table.Row{res.Name, res.Logs[idx].EventLogId, v.Id, v.StartOffsetInLog,
									v.EndOffsetInLog})
							} else if sIdx == 0 {
								t.AppendRow(table.Row{"", res.Logs[idx].EventLogId, v.Id, v.StartOffsetInLog,
									v.EndOffsetInLog})
							} else {
								t.AppendRow(table.Row{"", "", v.Id, v.StartOffsetInLog,
									v.EndOffsetInLog})
							}
						}
						t.AppendSeparator()
					}
					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 2, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 3, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 4, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 5, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
				} else {
					t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment", "Start", "End", "Block", "Leader", "Volume", "Endpoint"})
					for idx := 0; idx < len(res.Logs); idx++ {
						segOfEL := segs[res.Logs[idx].EventLogId]
						for sIdx, seg := range segOfEL {
							tIdx := 0
							for _, blk := range seg.Replicas {
								if idx == 0 && sIdx == 0 && tIdx == 0 {
									t.AppendRow(table.Row{res.Name, res.Logs[idx].EventLogId, seg.Id, seg.StartOffsetInLog,
										seg.EndOffsetInLog, blk.Id, blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
								} else if sIdx == 0 && tIdx == 0 {
									t.AppendRow(table.Row{"", res.Logs[idx].EventLogId, seg.Id, seg.StartOffsetInLog,
										seg.EndOffsetInLog, blk.Id, blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
								} else if tIdx == 0 {
									t.AppendRow(table.Row{"", "", seg.Id, seg.StartOffsetInLog,
										seg.EndOffsetInLog, blk.Id, blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
								} else {
									t.AppendRow(table.Row{"", "", "", "", "", blk.Id, blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
								}
								tIdx++
							}
						}
						t.AppendSeparator()
					}
					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 2, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 3, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 4, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 5, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 6, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 7, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 8, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 9, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
				}
			}
			t.SetStyle(table.StyleLight)
			t.Style().Options.SeparateRows = true
			t.Style().Box = table.StyleBoxDefault
			t.SetOutputMirror(os.Stdout)
			t.Render()
		},
	}
	cmd.Flags().BoolVar(&showSegment, "segment", false, "if show all segment of eventlog")
	cmd.Flags().BoolVar(&showBlock, "block", false, "if show all block of segment")
	return cmd
}

func listEventbusInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			grpcConn := mustGetLeaderControllerGRPCConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			res, err := cli.ListEventBus(ctx, &empty.Empty{})
			if err != nil {
				cmdFailedf("list eventbus failed: %s", err)
			}
			data, _ := json.Marshal(res)
			var out bytes.Buffer
			_ = json.Indent(&out, data, "", "\t")
			color.Green("%s", out.String())
		},
	}
	cmd.Flags().StringVar(&eventbus, "name", "", "eventbus name to deleting")
	return cmd
}
