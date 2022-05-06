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
	"fmt"
	ce "github.com/cloudevents/sdk-go/v2"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strings"
)

var (
	eventID           = ""
	eventSource       = ""
	eventType         = ""
	eventBody         = ""
	dataFile          = ""
	printDataTemplate bool
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
				fmt.Println("id1,source1,type1,data1")
				fmt.Println("id2,,,data2")
				fmt.Println(",,,data3")
				os.Exit(0)
			}
			endpoint, err := endpointsFromCmd(cmd)
			if err != nil {
				fmt.Printf("parse endpoints error: %s\n", err)
				os.Exit(-1)
			}
			if len(args) == 0 {
				fmt.Println("eventbus name can't be empty")
				fmt.Println()
				fmt.Printf("============ see below for right usage ============\n")
				fmt.Println()
				_ = cmd.Help()
				os.Exit(-1)
			}

			p, err := ce.NewHTTP()
			if err != nil {
				fmt.Printf("init ce protocol error: %s\n", err)
				os.Exit(-1)
			}
			c, err := ce.NewClient(p, ce.WithTimeNow(), ce.WithUUIDs())
			if err != nil {
				fmt.Printf("create ce client error: %s\n", err)
				os.Exit(-1)
			}
			ctx := ce.ContextWithTarget(context.Background(), fmt.Sprintf("http://%s/gateway/%s", endpoint, args[0]))

			if dataFile == "" {
				sendOne(ctx, c)
			} else {
				sendFile(ctx, c)
			}
		},
	}
	cmd.Flags().StringVar(&eventID, "id", "cmd", "event id of CloudEvent")
	cmd.Flags().StringVar(&eventSource, "source", "cmd", "event source of CloudEvent")
	cmd.Flags().StringVar(&eventType, "type", "cmd", "event type of CloudEvent")
	cmd.Flags().StringVar(&eventBody, "body", "", "event body of CloudEvent")
	cmd.Flags().StringVar(&dataFile, "data", "", "the data file to send, each line represent a event "+
		"and like [id],[source],[type],<body>")
	cmd.Flags().BoolVar(&printDataTemplate, "print-template", false, "print data template file")
	return cmd
}

func sendOne(ctx context.Context, ceClient ce.Client) {
	event := ce.NewEvent()
	event.SetID(eventID)
	event.SetSource(eventSource)
	event.SetType(eventType)
	err := event.SetData(ce.ApplicationJSON, eventBody)
	if err != nil {
		fmt.Printf("set data failed: %s\n", err)
		os.Exit(-1)
	}
	res := ceClient.Send(ctx, event)
	if ce.IsUndelivered(res) {
		fmt.Printf("failed to send: %s\n", res.Error())
		os.Exit(-1)
	} else {
		var httpResult *cehttp.Result
		ce.ResultAs(res, &httpResult)
		fmt.Printf("send %d \n", httpResult.StatusCode)
	}
}

func sendFile(ctx context.Context, ceClient ce.Client) {
	f, err := os.Open(dataFile)
	defer func() {
		_ = f.Close()
	}()
	if err != nil {
		fmt.Printf("open data file failed: %s\n", err)
		os.Exit(-1)
	}
	events := make([][]string, 0)
	reader := bufio.NewReader(f)
	for {
		data, isPrx, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("read data file failed: %s\n", err)
			os.Exit(-1)
		}
		for isPrx {
			var _data []byte
			_data, isPrx, err = reader.ReadLine()
			if err != nil {
				fmt.Printf("read data file failed: %s\n", err)
				os.Exit(-1)
			}
			data = append(_data, _data...)
		}
		arr := strings.Split(string(data), ",")
		if len(arr) != 4 {
			fmt.Printf("invalid data file: %s, the each line must be [id],[source],[type],<body>", string(data))
			os.Exit(-1)
		}
		events = append(events, arr)
	}
	for idx, v := range events {
		event := ce.NewEvent()
		event.SetID(v[0])
		event.SetSource(v[1])
		event.SetType(v[2])
		err := event.SetData(ce.ApplicationJSON, v[3])
		if err != nil {
			fmt.Printf("set data failed: %s\n", err)
			os.Exit(-1)
		}
		res := ceClient.Send(ctx, event)
		if ce.IsUndelivered(res) {
			fmt.Printf("failed to send: %s\n", res.Error())
			os.Exit(-1)
		} else {
			var httpResult *cehttp.Result
			ce.ResultAs(res, &httpResult)
			fmt.Printf("%dth sent %d \n", idx, httpResult.StatusCode)
		}
	}
}

func getEventCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <eventbus-name> ",
		Short: "get a event from specified eventbus",
		Run: func(cmd *cobra.Command, args []string) {
			println("get event")
		},
	}
	//cmd.Flags().String("eventlog", "", "specified eventlog id get from")
	cmd.Flags().Int64("offset", -1, "which position you want to start get")
	cmd.Flags().Int16("number", 1, "the number of event you want to get")
	return cmd
}
