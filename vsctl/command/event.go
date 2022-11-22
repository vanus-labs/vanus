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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	cloudEventDataRowLength = 4
	httpPrefix              = "http://"
	xceVanusDeliveryTime    = "xvanusdeliverytime"
)

func NewEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event sub-command ",
		Short: "convenient operations for pub/sub",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(getEventCommand())
	cmd.AddCommand(putEventCommand())
	return cmd
}

func putEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "put <eventbus-name> ",
		Short: "send a event to eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if printDataTemplate {
				color.White("id1,source1,type1,data1")
				color.White("id2,,,data2")
				color.White(",,,data3")
				os.Exit(0)
			}
			if len(args) == 0 {
				cmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
			}
			endpoint := mustGetGatewayCloudEventsEndpoint(cmd)
			p, err := v2.NewHTTP()
			if err != nil {
				cmdFailedf(cmd, "init ce protocol error: %s\n", err)
			}
			c, err := v2.NewClient(p, v2.WithTimeNow(), v2.WithUUIDs())
			if err != nil {
				cmdFailedf(cmd, "create ce client error: %s\n", err)
			}
			var target string
			if strings.HasPrefix(endpoint, httpPrefix) {
				target = fmt.Sprintf("%s/gateway/%s", endpoint, args[0])
			} else {
				target = fmt.Sprintf("%s%s/gateway/%s", httpPrefix, endpoint, args[0])
			}

			ctx := v2.ContextWithTarget(context.Background(), target)

			if dataFile == "" {
				sendOne(ctx, cmd, c)
			} else {
				sendFile(ctx, cmd, c)
			}
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "event id of CloudEvent")
	cmd.Flags().StringVar(&dataFormat, "data-format", "json", "the format of event body, JSON or plain")
	cmd.Flags().StringVar(&eventSource, "source", "cmd", "event source of CloudEvent")
	cmd.Flags().StringVar(&eventDeliveryTime, "delivery-time", "",
		"event delivery time of CloudEvent, only support the time layout of RFC3339, for example: 2022-01-01T08:00:00Z")
	cmd.Flags().StringVar(&eventDelayTime, "delay-time", "",
		"event delay delivery time of CloudEvent, only support the unit of seconds, for example: 60")
	cmd.Flags().StringVar(&eventType, "type", "cmd", "event type of CloudEvent")
	cmd.Flags().StringVar(&eventData, "data", "", "event data of CloudEvent")
	cmd.Flags().StringVar(&dataFile, "file", "", "the data file to send, each line represent a event "+
		"and like [id],[source],[type],<body>")
	cmd.Flags().BoolVar(&printDataTemplate, "print-template", false, "print data template file")
	cmd.Flags().BoolVar(&detail, "detail", false, "show detail of persistence event")
	return cmd
}

func sendOne(ctx context.Context, cmd *cobra.Command, ceClient v2.Client) {
	event := v2.NewEvent()
	if id == "" {
		id = uuid.NewString()
	}
	event.SetID(id)
	event.SetSource(eventSource)
	event.SetType(eventType)
	if eventDeliveryTime != "" {
		// validate event delivery time
		if _, err := time.Parse(time.RFC3339Nano, eventDeliveryTime); err != nil {
			cmdFailedf(cmd, "invalid format of delivery-time: %s\n", err)
		}
		event.SetExtension(xceVanusDeliveryTime, eventDeliveryTime)
	} else if eventDelayTime != "" {
		// validate event delay time
		timeOfInt64, err := strconv.ParseInt(eventDelayTime, 10, 64)
		if err != nil {
			cmdFailedf(cmd, "invalid format of delay-time: %s\n", err)
		}
		timeOfRFC3339Nano := time.Now().Add(time.Duration(timeOfInt64) * time.Second).Format(time.RFC3339Nano)
		event.SetExtension(xceVanusDeliveryTime, timeOfRFC3339Nano)
	}
	var err error
	if strings.ToLower(dataFormat) == "json" {
		m := make(map[string]interface{})
		if err := json.Unmarshal([]byte(eventData), &m); err != nil {
			color.White(eventData)
			cmdFailedf(cmd, "invalid format of data body: %s, err: %s", eventData, err.Error())
		}
		err = event.SetData(v2.ApplicationJSON, m)
	} else {
		err = event.SetData(v2.TextPlain, eventData)
	}

	if err != nil {
		cmdFailedf(cmd, "set data failed: %s\n", err)
	}

	var res protocol.Result
	var resEvent *v2.Event
	if !detail {
		res = ceClient.Send(ctx, event)
	} else {
		resEvent, res = ceClient.Request(ctx, event)
	}

	if v2.IsUndelivered(res) {
		cmdFailedf(cmd, "failed to send: %s\n", res.Error())
	} else {
		var httpResult *cehttp.Result
		v2.ResultAs(res, &httpResult)
		if httpResult == nil {
			cmdFailedf(cmd, "failed to send: %s\n", res.Error())
		} else {
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{
					"Result": httpResult.StatusCode,
					"Error":  fmt.Errorf(httpResult.Format, httpResult.Args...),
				})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				tbcfg := []table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				}
				if detail {
					t.AppendHeader(table.Row{"Result", "RESPONSE EVENT"})
					t.AppendRow(table.Row{httpResult.StatusCode, resEvent})
					tbcfg = append(tbcfg, table.ColumnConfig{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter})
				} else {
					t.AppendHeader(table.Row{"Result", "Error"})
					t.AppendRow(table.Row{httpResult.StatusCode, fmt.Errorf(httpResult.Format, httpResult.Args...)})
				}
				t.SetColumnConfigs(tbcfg)
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		}
	}
}

func sendFile(ctx context.Context, cmd *cobra.Command, ceClient v2.Client) {
	f, err := os.Open(dataFile)
	defer func() {
		_ = f.Close()
	}()
	if err != nil {
		cmdFailedf(cmd, "open data file failed: %s\n", err)
	}
	events := make([][]string, 0)
	reader := bufio.NewReader(f)
	for {
		data, isPrx, _err := reader.ReadLine()
		if _err != nil {
			if _err == io.EOF {
				break
			}
			cmdFailedf(cmd, "read data file failed: %s\n", _err)
		}
		for isPrx {
			var _data []byte
			_data, isPrx, err = reader.ReadLine()
			if err != nil {
				cmdFailedf(cmd, "read data file failed: %s\n", err)
			}
			data = append(data, _data...)
		}
		arr := strings.Split(string(data), ",")
		if len(arr) != cloudEventDataRowLength {
			cmdFailedf(cmd, "invalid data file: %s, please see vsctl event put --print-template", string(data))
		}
		events = append(events, arr)
	}
	t := table.NewWriter()
	tbcfg := []table.ColumnConfig{
		{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	}
	if detail {
		t.AppendHeader(table.Row{"No.", "Result", "Response Event"})
		tbcfg = append(tbcfg, table.ColumnConfig{Number: 3, Align: text.AlignCenter, AlignHeader: text.AlignCenter})
	} else {
		t.AppendHeader(table.Row{"No.", "Result"})
	}
	t.SetColumnConfigs(tbcfg)
	t.SetOutputMirror(os.Stdout)
	for idx, v := range events {
		event := v2.NewEvent()
		event.SetID(v[0])
		event.SetSource(v[1])
		event.SetType(v[2])
		err = event.SetData(v2.ApplicationJSON, v[3])
		if err != nil {
			cmdFailedf(cmd, "set data failed: %s\n", err)
		}

		var res protocol.Result
		var resEvent *v2.Event
		if !detail {
			resEvent, res = nil, ceClient.Send(ctx, event)
		} else {
			resEvent, res = ceClient.Request(ctx, event)
		}

		if v2.IsUndelivered(res) {
			cmdFailedf(cmd, "failed to send: %s\n", res.Error())
		} else {
			var httpResult *cehttp.Result
			v2.ResultAs(res, &httpResult)
			if httpResult == nil {
				cmdFailedf(cmd, "failed to send: %s\n", res.Error())
			} else {
				if IsFormatJSON(cmd) {
					data, _ := json.Marshal(map[string]interface{}{
						"No.":    idx,
						"Result": httpResult.StatusCode,
					})
					color.Green(string(data))
				} else {
					if detail {
						t.AppendRow(table.Row{idx, httpResult.StatusCode, resEvent})
					} else {
						t.AppendRow(table.Row{idx, httpResult.StatusCode})
					}
					t.AppendSeparator()
					t.Render()
				}
			}
		}
	}
}

func getEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <eventbus-name or event-id> ",
		Short: "get a event from specified eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 && eventID == "" {
				cmdFailedWithHelpNotice(cmd, "eventbus name and eventID can't be both empty\n")
			}

			res, err := client.GetEvent(context.Background(), &proxypb.GetEventRequest{
				Eventbus:   args[0],
				EventlogId: eventlogID,
				Offset:     offset,
				EventId:    eventID,
				Number:     int32(number),
			})

			if err != nil {
				cmdFailedf(cmd, "send request to gateway failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				for idx := range res.Events {
					data, _ := json.Marshal(map[string]interface{}{
						"No.":   idx,
						"Event": res.Events[idx].String(),
					})

					color.Yellow(string(data))
				}
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"No.", "Event"})
				for idx := range res.Events {
					t.AppendRow(table.Row{idx, format(res.Events[idx])})
					t.AppendSeparator()
				}
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}

	cmd.Flags().Int64Var(&offset, "offset", 0, "which position you want to start get")
	cmd.Flags().Int16Var(&number, "number", 1, "the number of event you want to get")
	cmd.Flags().StringVar(&eventID, "event-id", "", "get event by event ID")
	cmd.Flags().StringVar(&eventlogID, "eventlog-id", "", "get events from a specified eventlog")
	return cmd
}

func format(value *wrapperspb.BytesValue) string {
	e := v2.NewEvent()
	_ = e.UnmarshalJSON(value.Value)
	return e.String()
}
