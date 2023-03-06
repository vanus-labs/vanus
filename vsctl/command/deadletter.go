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
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

func newDeadLetterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dead-letter sub-command ",
		Short: "sub-commands for dead letter operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
	}
	cmd.AddCommand(getDeadLetterCommand())
	cmd.AddCommand(resendDeadLetterCommand())
	return cmd
}

func getDeadLetterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get",
		Short: "get a subscription dead letter event",
		Run: func(cmd *cobra.Command, args []string) {
			id, err := vanus.NewIDFromString(subscriptionIDStr)
			if err != nil {
				cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid subscription id: %s\n", err.Error()))
			}

			res, err := client.GetDeadLetterEvent(context.Background(), &proxypb.GetDeadLetterEventRequest{
				SubscriptionId: id.Uint64(),
				Offset:         uint64(offset),
				Number:         int32(number),
			})
			if err != nil {
				cmdFailedf(cmd, "failed to get event: %s", err)
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

	cmd.Flags().StringVar(&subscriptionIDStr, "id", "", "subscription id")
	cmd.Flags().Int64Var(&offset, "offset", 0, "which position you want to start get")
	cmd.Flags().Int16Var(&number, "number", 1, "the number of event you want to get")
	return cmd
}

func resendDeadLetterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resend",
		Short: "resend a subscription dead letter event",
		Run: func(cmd *cobra.Command, args []string) {
			id, err := vanus.NewIDFromString(subscriptionIDStr)
			if err != nil {
				cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid subscription id: %s\n", err.Error()))
			}

			_, err = client.ResendDeadLetterEvent(context.Background(), &proxypb.ResendDeadLetterEventRequest{
				SubscriptionId: id.Uint64(),
				StartOffset:    startOffset,
				EndOffset:      endOffset,
			})

			if err != nil {
				cmdFailedf(cmd, "failed to resend: %s\n", err)
			}
			color.Green("resend event success")
		},
	}

	cmd.Flags().StringVar(&subscriptionIDStr, "id", "", "subscription id")
	cmd.Flags().Uint64Var(&startOffset, "start", 0, "which position you want to start get, default first")
	cmd.Flags().Uint64Var(&endOffset, "end", 0, "which position you want to end get, default to end")
	return cmd
}
