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
	"fmt"
	"os"
	"strings"

	ce "github.com/cloudevents/sdk-go/v2"
	eb "github.com/linkall-labs/eventbus-go"
	"github.com/spf13/cobra"
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
			eps, err := endpointsFromCmd(cmd)
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
			vrn := fmt.Sprintf("vanus://%s/eventbus/%s?controllers=%s", "", args[0],
				strings.Join(eps, ","))
			writer, err := eb.OpenBusWriter(vrn)
			defer writer.Close()
			if err != nil {
				fmt.Printf("open eventbus writer error: %s\n", err)
				os.Exit(-1)
			}
			ctx := context.Background()
			event := ce.NewEvent()
			event.SetID(eventID)
			event.SetSource(eventSource)
			event.SetType(eventType)
			err = event.SetData(ce.ApplicationJSON, eventBody)
			_, err = writer.Append(ctx, &event)
			if err != nil {
				fmt.Printf("send event to eventbus error: %s\n", err)
				os.Exit(-1)
			} else {
				fmt.Println("send event to eventbus success")
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
