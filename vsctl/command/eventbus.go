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
	"context"
	"encoding/json"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"github.com/spf13/cobra"
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
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			_, err := cli.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name: eventbus,
			})
			if err != nil {
				cmdFailedf(cmd, "create eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Success", "Eventbus": eventbus})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "Eventbus"})
				t.AppendRow(table.Row{"Create Success", eventbus})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
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
				cmdFailedf(cmd, "the --name flag MUST be set")
			}
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			_, err := cli.DeleteEventBus(ctx, &metapb.EventBus{Name: eventbus})
			if err != nil {
				cmdFailedf(cmd, "delete eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Success", "Eventbus": eventbus})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "Eventbus"})
				t.AppendRow(table.Row{"Delete Success", eventbus})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&eventbus, "name", "", "eventbus name to deleting")
	return cmd
}

func getEventbusInfoCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info [flag] ",
		Short: "get the eventbus info",
		Run: func(cmd *cobra.Command, args []string) {
			if eventbus == "" && (len(args) == 0 || args[0] == "") {
				cmdFailedf(cmd, "the eventbus must be set")
			}
			var buses []string
			if len(args) > 0 && args[0] != "" {
				buses = append(buses, args[0])
			} else {
				buses = strings.Split(eventbus, ",")
			}
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			busMetas := make([]*metapb.EventBus, 0)
			segs := make(map[uint64][]*metapb.Segment)
			for idx := range buses {
				res, err := cli.GetEventBus(ctx, &metapb.EventBus{Name: buses[idx]})
				if err != nil {
					cmdFailedf(cmd, "get eventbus failed: %s", err)
				}

				busMetas = append(busMetas, res)
				if showSegment || showBlock {
					logCli := ctrlpb.NewEventLogControllerClient(grpcConn)
					logs := res.GetLogs()
					for idx := range logs {
						segRes, err := logCli.ListSegment(ctx, &ctrlpb.ListSegmentRequest{
							EventBusId: res.Id,
							EventLogId: logs[idx].EventLogId,
						})
						if err != nil {
							cmdFailedf(cmd, "get segments failed: %s", err)
						}
						segs[logs[idx].EventLogId] = segRes.Segments
					}
				}
			}

			t := table.NewWriter()
			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			}
			if !showSegment && !showBlock {
				t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment Number"})
				for _, res := range busMetas {
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
						{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
				}
			} else {
				if !showBlock {
					t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment", "Capacity", "Size", "Start", "End"})
					for _, res := range busMetas {
						for idx := 0; idx < len(res.Logs); idx++ {
							segOfEL := segs[res.Logs[idx].EventLogId]
							for sIdx, v := range segOfEL {
								switch {
								case idx == 0 && sIdx == 0:
									t.AppendRow(table.Row{res.Name, res.Logs[idx].EventLogId, v.Id, v.Capacity, v.Size,
										v.StartOffsetInLog, v.EndOffsetInLog})
								case sIdx == 0:
									t.AppendRow(table.Row{"", res.Logs[idx].EventLogId, v.Id, v.Capacity, v.Size,
										v.StartOffsetInLog, v.EndOffsetInLog})
								default:
									t.AppendRow(table.Row{"", "", v.Id, v.Capacity, v.Size, v.StartOffsetInLog,
										v.EndOffsetInLog})
								}
							}
							t.AppendSeparator()
						}
					}

					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 2, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 3, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 4, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 5, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 6, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 7, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					})
				} else {
					t.AppendHeader(table.Row{"Eventbus", "Eventlog", "Segment", "Capacity", "Size", "Start", "End",
						"Block", "Leader", "Volume", "Endpoint"})
					multiReplica := false
					for _, res := range busMetas {
						for idx := 0; idx < len(res.Logs); idx++ {
							segOfEL := segs[res.Logs[idx].EventLogId]
							for sIdx, seg := range segOfEL {
								tIdx := 0
								if !multiReplica && len(seg.Replicas) > 1 {
									multiReplica = true
								}
								var vols []uint64
								var volMap = map[uint64]*metapb.Block{}
								for _, v := range seg.Replicas {
									vols = append(vols, v.VolumeID)
									volMap[v.VolumeID] = v
								}
								sortkeys.Uint64s(vols)
								for _, k := range vols {
									blk := volMap[k]
									switch {
									case idx == 0 && sIdx == 0 && tIdx == 0:
										t.AppendRow(table.Row{res.Name, res.Logs[idx].EventLogId, seg.Id, seg.Capacity,
											seg.Size, seg.StartOffsetInLog, seg.EndOffsetInLog, blk.Id,
											blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
									case sIdx == 0 && tIdx == 0:
										t.AppendRow(table.Row{"", res.Logs[idx].EventLogId, seg.Id, seg.Capacity,
											seg.Size, seg.StartOffsetInLog, seg.EndOffsetInLog, blk.Id,
											blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
									case tIdx == 0:
										t.AppendRow(table.Row{"", "", seg.Id, seg.Capacity, seg.Size,
											seg.StartOffsetInLog, seg.EndOffsetInLog, blk.Id,
											blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
									default:
										t.AppendRow(table.Row{"", "", "", "", "", "", "", blk.Id,
											blk.Id == seg.LeaderBlockId, blk.VolumeID, blk.Endpoint})
									}

									tIdx++
								}
							}
							t.AppendSeparator()
						}
					}
					t.SetColumnConfigs([]table.ColumnConfig{
						{Number: 1, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 2, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 3, VAlign: text.VAlignMiddle, AutoMerge: true, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 4, VAlign: text.VAlignMiddle, AutoMerge: multiReplica, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 5, VAlign: text.VAlignMiddle, AutoMerge: multiReplica, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 6, VAlign: text.VAlignMiddle, AutoMerge: multiReplica, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 7, VAlign: text.VAlignMiddle, AutoMerge: multiReplica, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter},
						{Number: 8, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 9, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 10, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: 11, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
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
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus to show, use , to separate")
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
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewEventBusControllerClient(grpcConn)
			res, err := cli.ListEventBus(ctx, &empty.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Name", "ID", "Eventlog", "Segments"})
				for idx := range res.Eventbus {
					eb := res.Eventbus[idx]
					if len(eb.Logs) == 0 {
						t.AppendRow(table.Row{eb.Name, eb.Id})
					} else {
						t.AppendRow(table.Row{eb.Name, eb.Id, eb.Logs[0].EventLogId, eb.Logs[0].CurrentSegmentNumbers})
						for sIdx := 1; sIdx < len(eb.Logs); sIdx++ {
							t.AppendSeparator()
							t.AppendRow(table.Row{eb.Name, eb.Id, eb.Logs[idx].EventLogId, eb.Logs[idx].CurrentSegmentNumbers})
						}
					}
					t.AppendSeparator()
				}
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 4, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}
