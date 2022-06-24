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
	"net/http"
	"os"
	"strconv"
	"strings"

	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
)

const (
	cloudEventDataRowLength = 4
	httpPrefix              = "http://"
)

func NewEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event sub-command ",
		Short: "convenient operations for pub/sub",
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
			endpoint := mustGetGatewayEndpoint(cmd)
			p, err := ce.NewHTTP()
			if err != nil {
				cmdFailedf(cmd, "init ce protocol error: %s\n", err)
			}
			c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
			if err != nil {
				cmdFailedf(cmd, "create ce client error: %s\n", err)
			}
			var target string
			if strings.HasPrefix(endpoint, httpPrefix) {
				target = fmt.Sprintf("%s/gateway/%s", endpoint, args[0])
			} else {
				target = fmt.Sprintf("%s%s/gateway/%s", httpPrefix, endpoint, args[0])
			}

			ctx := ce.ContextWithTarget(context.Background(), target)

			if dataFile == "" {
				sendOne(cmd, ctx, c)
			} else {
				sendFile(cmd, ctx, c)
			}
		},
	}
	cmd.Flags().StringVar(&eventID, "id", "", "event id of CloudEvent")
	cmd.Flags().StringVar(&dataFormat, "data-format", "json", "the format of event body, JSON or plain")
	cmd.Flags().StringVar(&eventSource, "source", "cmd", "event source of CloudEvent")
	cmd.Flags().StringVar(&eventType, "type", "cmd", "event type of CloudEvent")
	cmd.Flags().StringVar(&eventBody, "body", "", "event body of CloudEvent")
	cmd.Flags().StringVar(&dataFile, "data", "", "the data file to send, each line represent a event "+
		"and like [id],[source],[type],<body>")
	cmd.Flags().BoolVar(&printDataTemplate, "print-template", false, "print data template file")
	return cmd
}

func sendOne(cmd *cobra.Command, ctx context.Context, ceClient ce.Client) {
	event := ce.NewEvent()
	if eventID == "" {
		eventID = uuid.NewString()
	}
	event.SetID(eventID)
	event.SetSource(eventSource)
	event.SetType(eventType)
	var err error
	if strings.ToLower(dataFormat) == "json" {
		m := make(map[string]interface{})
		if err := json.Unmarshal([]byte(eventBody), &m); err != nil {
			color.White(eventBody)
			cmdFailedf(cmd, "invalid format of data body: %s, err: %s", eventBody, err.Error())
		}
		err = event.SetData(ce.ApplicationJSON, m)
	} else {
		err = event.SetData(ce.TextPlain, eventBody)
	}

	if err != nil {
		cmdFailedf(cmd, "set data failed: %s\n", err)
	}
	res := ceClient.Send(ctx, event)
	if ce.IsUndelivered(res) {
		cmdFailedf(cmd, "failed to send: %s\n", res.Error())
	} else {
		var httpResult *cehttp.Result
		ce.ResultAs(res, &httpResult)
		if httpResult == nil {
			cmdFailedf(cmd, "failed to send: %s\n", res.Error())
		} else {
			if isOutputFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"Result": httpResult.StatusCode})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"Result"})
				t.AppendRow(table.Row{httpResult.StatusCode})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
					{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		}
	}
}

func sendFile(cmd *cobra.Command, ctx context.Context, ceClient ce.Client) {
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
	t.AppendHeader(table.Row{"No.", "Result"})
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
		{Number: 2, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
	})
	t.SetOutputMirror(os.Stdout)
	for idx, v := range events {
		event := ce.NewEvent()
		event.SetID(v[0])
		event.SetSource(v[1])
		event.SetType(v[2])
		err = event.SetData(ce.ApplicationJSON, v[3])
		if err != nil {
			cmdFailedf(cmd, "set data failed: %s\n", err)
		}
		res := ceClient.Send(ctx, event)
		if ce.IsUndelivered(res) {
			cmdFailedf(cmd, "failed to send: %s\n", res.Error())
		} else {
			var httpResult *cehttp.Result
			ce.ResultAs(res, &httpResult)
			if httpResult == nil {
				cmdFailedf(cmd, "failed to send: %s\n", res.Error())
			} else {
				if isOutputFormatJSON(cmd) {
					data, _ := json.Marshal(map[string]interface{}{
						"No.":    idx,
						"Result": httpResult.StatusCode,
					})
					color.Green(string(data))
				} else {
					t.AppendRow(table.Row{idx, httpResult.StatusCode})
					t.AppendSeparator()
					t.Render()
				}
			}
		}
	}
}

func getEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <eventbus-name> ",
		Short: "get a event from specified eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
			}
			endpoint := mustGetGatewayEndpoint(cmd)
			if !strings.HasPrefix(endpoint, httpPrefix) {
				endpoint = httpPrefix + endpoint
			}
			idx := strings.LastIndex(endpoint, ":")
			port, err := strconv.Atoi(endpoint[idx+1:])
			if err != nil {
				cmdFailedf(cmd, "parse gateway port failed: %s, endpoint: %s", err, endpoint)
			}
			endpoint = fmt.Sprintf("%s:%d", endpoint[:idx], port+1)
			res, err := newHTTPRequest().Get(fmt.Sprintf("%s/getEvents?eventbus=%s&offset=%d&number=%d",
				endpoint, args[0], offset, number))
			if err != nil {
				cmdFailedf(cmd, "send request to gateway failed: %s", err)
			}
			if res.StatusCode() != http.StatusOK {
				cmdFailedf(cmd, "got response, but no 200 OK: %d", res.StatusCode())
			}
			data := new(struct {
				Events []ce.Event
			})
			err = json.Unmarshal(res.Body(), data)
			if err != nil {
				cmdFailedf(cmd, "unmarshal http response data failed: %s", err)
			}
			if isOutputFormatJSON(cmd) {
				for idx := range data.Events {
					data, _ := json.Marshal(map[string]interface{}{
						"No.":   idx,
						"Event": data.Events[idx].String(),
					})

					color.Yellow(string(data))
				}
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"No.", "Event"})
				for idx := range data.Events {
					t.AppendRow(table.Row{idx, data.Events[idx].String()})
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

	// TODO cmd.Flags().String("eventlog", "", "specified eventlog id get from")

	cmd.Flags().Int64Var(&offset, "offset", 0, "which position you want to start get")
	cmd.Flags().Int16Var(&number, "number", 1, "the number of event you want to get")
	return cmd
}
