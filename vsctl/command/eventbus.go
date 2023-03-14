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

package command

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

func NewEventbusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "eventbus sub-command",
		Short: "sub-commands for eventbus operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
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
			_, err := client.CreateEventbus(context.Background(), &ctrlpb.CreateEventbusRequest{
				Name:        eventbus,
				LogNumber:   eventlogNum,
				Description: description,
				NamespaceId: mustGetNamespaceID(primitive.DefaultNamespace).Uint64(),
			})
			if err != nil {
				cmdFailedf(cmd, "create eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Create Success", "EventbusService": eventbus})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "EventbusService"})
				t.AppendRow(table.Row{"Success to create eventbus into [ default ] namespace", eventbus})
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
	cmd.Flags().Int32Var(&eventlogNum, "eventlog", 1, "number of eventlog")
	cmd.Flags().StringVar(&description, "description", "", "subscription description")
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

			_, err := client.DeleteEventbus(context.Background(),
				wrapperspb.UInt64(mustGetEventbusID(namespace, eventbus).Uint64()))
			if err != nil {
				cmdFailedf(cmd, "delete eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": "Delete Success", "EventbusService": eventbus})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result", "EventbusService"})
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

			segs := make(map[uint64][]*metapb.Segment)
			ctx := context.Background()
			res, err := client.GetEventbus(ctx,
				wrapperspb.UInt64(mustGetEventbusID(namespace, args[0]).Uint64()))
			if err != nil {
				cmdFailedf(cmd, "get eventbus failed: %s", err)
			}

			if showSegment || showBlock {
				logs := res.GetLogs()
				for idx := range logs {
					segRes, err := client.ListSegment(ctx, &ctrlpb.ListSegmentRequest{
						EventbusId: res.Id,
						EventlogId: logs[idx].EventlogId,
					})
					if err != nil {
						cmdFailedf(cmd, "get segments failed: %s", err)
					}
					segs[logs[idx].EventlogId] = segRes.Segments
				}
			}

			t := table.NewWriter()
			if IsFormatJSON(cmd) {
				color.Yellow("WARN: this command doesn't support --output-format\n")
			}
			if !showSegment && !showBlock {
				t.AppendHeader(table.Row{
					"EventbusService", "Description", "Created_At", "Updated_At",
					"Eventlog", "Segment Number",
				})
				for idx := 0; idx < len(res.Logs); idx++ {
					if idx == 0 {
						t.AppendRow(table.Row{
							res.Name,
							res.Description,
							time.UnixMilli(res.CreatedAt).Format(time.RFC3339),
							time.UnixMilli(res.UpdatedAt).Format(time.RFC3339),
							formatID(res.Logs[idx].EventlogId),
							res.Logs[idx].CurrentSegmentNumbers,
						})
					}
				}
				cfgs := eventbusColConfigs()
				cfgs = append(cfgs, table.ColumnConfig{
					Number:      len(cfgs) + 1,
					VAlign:      text.VAlignMiddle,
					Align:       text.AlignCenter,
					AlignHeader: text.AlignCenter,
				})
				t.SetColumnConfigs(cfgs)
			} else {
				if !showBlock {
					t.AppendHeader(table.Row{
						"EventbusService", "Description", "Created_At", "Updated_At",
						"Eventlog", "Segment", "Capacity", "Size", "Start", "End",
					})
					for idx := 0; idx < len(res.Logs); idx++ {
						segOfEL := segs[res.Logs[idx].EventlogId]
						for _, v := range segOfEL {
							t.AppendRow(table.Row{
								res.Name, res.Description,
								time.UnixMilli(res.CreatedAt).Format(time.RFC3339),
								time.UnixMilli(res.UpdatedAt).Format(time.RFC3339),
								formatID(res.Logs[idx].EventlogId), formatID(v.Id),
								v.Capacity, v.Size, v.StartOffsetInLog, v.EndOffsetInLog,
							})
						}
						t.AppendSeparator()
					}

					cfgs := eventbusColConfigs()
					cfgs = append(cfgs, []table.ColumnConfig{
						{
							Number: len(cfgs) + 1, VAlign: text.VAlignMiddle,
							AutoMerge: true, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 2, VAlign: text.VAlignMiddle, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 3, VAlign: text.VAlignMiddle, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{Number: len(cfgs) + 4, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: len(cfgs) + 5, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					}...)
					t.SetColumnConfigs(cfgs)
				} else {
					t.AppendHeader(table.Row{
						"EventbusService", "Description", "Created_At", "Updated_At", "Eventlog",
						"Segment", "Capacity", "Size", "Start", "End", "Block", "Leader", "Volume", "Endpoint",
					})
					multiReplica := false
					for idx := 0; idx < len(res.Logs); idx++ {
						segOfEL := segs[res.Logs[idx].EventlogId]
						for _, seg := range segOfEL {
							if !multiReplica && len(seg.Replicas) > 1 {
								multiReplica = true
							}
							var vols []uint64
							volMap := map[uint64]*metapb.Block{}
							for _, v := range seg.Replicas {
								vols = append(vols, v.VolumeID)
								volMap[v.VolumeID] = v
							}
							sortkeys.Uint64s(vols)
							for _, k := range vols {
								blk := volMap[k]
								t.AppendRow(table.Row{
									res.Name, res.Description,
									time.UnixMilli(res.CreatedAt).Format(time.RFC3339),
									time.UnixMilli(res.UpdatedAt).Format(
										time.RFC3339), formatID(res.Logs[idx].EventlogId),
									formatID(seg.Id), seg.Capacity, seg.Size, seg.StartOffsetInLog,
									seg.EndOffsetInLog, formatID(blk.Id), blk.Id == seg.LeaderBlockId,
									blk.VolumeID, blk.Endpoint,
								})
							}
						}
						t.AppendSeparator()
					}
					cfgs := eventbusColConfigs()
					cfgs = append(cfgs, []table.ColumnConfig{
						{
							Number: len(cfgs) + 1, VAlign: text.VAlignMiddle,
							AutoMerge: true, Align: text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 2, VAlign: text.VAlignMiddle, AutoMerge: multiReplica,
							Align:       text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 3, VAlign: text.VAlignMiddle, AutoMerge: multiReplica,
							Align:       text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 4, VAlign: text.VAlignMiddle, AutoMerge: multiReplica,
							Align:       text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{
							Number: len(cfgs) + 5, VAlign: text.VAlignMiddle, AutoMerge: multiReplica,
							Align:       text.AlignCenter,
							AlignHeader: text.AlignCenter,
						},
						{Number: len(cfgs) + 6, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: len(cfgs) + 7, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: len(cfgs) + 8, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
						{Number: len(cfgs) + 9, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					}...)
					t.SetColumnConfigs(cfgs)
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
			res, err := client.ListEventbus(context.Background(), &ctrlpb.ListEventbusRequest{})
			if err != nil {
				cmdFailedf(cmd, "list eventbus failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{
					"Name", "Description", "Created_At",
					"Updated_At", "Eventlog Number",
				})
				for idx := range res.Eventbus {
					eb := res.Eventbus[idx]
					t.AppendSeparator()
					t.AppendRow(table.Row{
						eb.Name,
						eb.Description,
						time.UnixMilli(eb.CreatedAt).Format(time.RFC3339),
						time.UnixMilli(eb.UpdatedAt).Format(time.RFC3339),
						eb.LogNumber,
					})
				}
				cfgs := eventbusColConfigs()
				for idx := range cfgs {
					cfgs[idx].AutoMerge = false
				}
				t.SetColumnConfigs(cfgs)
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}

func eventbusColConfigs() []table.ColumnConfig {
	return []table.ColumnConfig{
		{Number: 1, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 2, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 3, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 4, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 5, AutoMerge: true, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	}
}

func formatID(id uint64) string {
	return vanus.NewIDFromUint64(id).String()
}
