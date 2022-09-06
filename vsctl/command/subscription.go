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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	"github.com/spf13/cobra"
)

func NewSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription sub-command ",
		Short: "sub-commands for subscription operations",
	}
	cmd.AddCommand(createSubscriptionCommand())
	cmd.AddCommand(deleteSubscriptionCommand())
	cmd.AddCommand(getSubscriptionCommand())
	cmd.AddCommand(listSubscriptionCommand())
	return cmd
}

func createSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if eventbus == "" {
				cmdFailedWithHelpNotice(cmd, "eventbus name can't be empty\n")
			}
			if sink == "" {
				cmdFailedWithHelpNotice(cmd, "sink name can't be empty\n")
			}

			var p meta.Protocol
			switch subProtocol {
			case "http", "":
				p = meta.Protocol_HTTP
			case "aws-lambda":
				p = meta.Protocol_AWS_LAMBDA
				if _, err := arn.Parse(sink); err != nil {
					cmdFailedf(cmd, "protocol is aws-lambda sink is aws arn, arn parse error: %s\n", err.Error())
				}
				if sinkCredentialType != "cloud" {
					cmdFailedf(cmd, "protocol is aws-lambda, credential-type must be cloud\n")
				}
			default:
				cmdFailedf(cmd, "protocol is invalid\n")
			}

			var credential *meta.SinkCredential
			if sinkCredentialType != "" {
				if sinkCredential == "" {
					cmdFailedf(cmd, "credential-type is set but credential empty\n")
				}
				switch sinkCredentialType {
				case "cloud":
					var cloud *meta.CloudCredential
					err := json.Unmarshal([]byte(sinkCredential), &cloud)
					if err != nil {
						cmdFailedf(cmd, "the sink credential unmarshal json error: %s", err.Error())
					}
					if cloud.AccessKeyId == "" || cloud.SecretAccessKey == "" {
						cmdFailedf(cmd, "credential-type is cloud, access_key_id and secret_access_key must not be empty\n")
					}
					credential = &meta.SinkCredential{
						CredentialType: meta.SinkCredential_CLOUD,
						Credential: &meta.SinkCredential_Cloud{
							Cloud: cloud,
						},
					}
				default:
					cmdFailedf(cmd, "credential-type is invalid\n")
				}
			}

			var filter []*meta.Filter
			if filters != "" {
				err := json.Unmarshal([]byte(filters), &filter)
				if err != nil {
					cmdFailedf(cmd, "the filter invalid: %s", err)
				}
			}

			var trans *meta.Transformer
			if transformer != "" {
				err := json.Unmarshal([]byte(transformer), &trans)
				if err != nil {
					cmdFailedf(cmd, "the transformer invalid: %s", err)
				}
			}

			// subscription config
			config := &meta.SubscriptionConfig{
				RateLimit:        rateLimit,
				DeliveryTimeout:  deliveryTimeout,
				MaxRetryAttempts: maxRetryAttempts,
			}
			if from != "" {
				switch from {
				case "latest":
					config.OffsetType = meta.SubscriptionConfig_LATEST
				case "earliest":
					config.OffsetType = meta.SubscriptionConfig_EARLIEST
				default:
					t, err := time.Parse(time.RFC3339, from)
					if err != nil {
						cmdFailedf(cmd, "consumer from time format is invalid: %s", err)
					}
					ts := uint64(t.Unix())
					config.OffsetTimestamp = &ts
					config.OffsetType = meta.SubscriptionConfig_TIMESTAMP
				}
			}

			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()
			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
				Subscription: &ctrlpb.SubscriptionRequest{
					Source:         source,
					Filters:        filter,
					Sink:           sink,
					Protocol:       p,
					SinkCredential: credential,
					EventBus:       eventbus,
					Transformer:    trans,
					Config:         config,
				},
			})
			if err != nil {
				cmdFailedf(cmd, "create subscription failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(getSubscriptionHeader())
				t.AppendRow(getSubscriptionRow(res))
				t.SetColumnConfigs(getSubscriptionColumnConfig())
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus name to consuming")
	cmd.Flags().StringVar(&sink, "sink", "", "the event you want to send to")
	cmd.Flags().StringVar(&filters, "filters", "", "filter event you interested, JSON format required")
	cmd.Flags().StringVar(&transformer, "transformer", "", "transformer, JSON format required")
	cmd.Flags().Int32Var(&rateLimit, "rate-limit", 0, "rate limit")
	cmd.Flags().StringVar(&from, "from", "", "consume events from, latest,earliest or RFC3339 format time")
	cmd.Flags().StringVar(&subProtocol, "protocol", "http", "protocol,http or aws-lambda")
	cmd.Flags().StringVar(&sinkCredentialType, "credential-type", "", "sink credential type, plain or cloud, now only support cloud")
	cmd.Flags().StringVar(&sinkCredential, "credential", "", "sink credential info, JSON format, "+
		"when credential-type is cloud, need access_key_id and secret_access_key")
	cmd.Flags().Int32Var(&deliveryTimeout, "delivery-timeout", 0, "event delivery to sink timeout, unit millisecond")
	cmd.Flags().Int32Var(&maxRetryAttempts, "max-retry-attempts", 0, "event delivery fail max retry attempts")
	return cmd
}

func deleteSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			if subscriptionID == 0 {
				cmdFailedWithHelpNotice(cmd, "subscriptionID name can't be empty\n")
			}
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			_, err := cli.DeleteSubscription(ctx, &ctrlpb.DeleteSubscriptionRequest{
				Id: subscriptionID,
			})
			if err != nil {
				cmdFailedf(cmd, "delete subscription failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"subscription_id": subscriptionID})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"subscriptionID"})
				t.AppendRow(table.Row{subscriptionID})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
			color.Green("delete subscription: %d success\n", subscriptionID)
		},
	}
	cmd.Flags().Uint64Var(&subscriptionID, "id", 0, "subscription id to deleting")
	return cmd
}

func getSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "get the subscription info ",
		Run: func(cmd *cobra.Command, args []string) {
			if subscriptionID == 0 {
				cmdFailedWithHelpNotice(cmd, "subscriptionID name can't be empty\n")
			}
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{
				Id: subscriptionID,
			})
			if err != nil {
				cmdFailedf(cmd, "get subscription info failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(getSubscriptionHeader())
				t.AppendRow(getSubscriptionRow(res))
				t.SetColumnConfigs(getSubscriptionColumnConfig())
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	cmd.Flags().Uint64Var(&subscriptionID, "id", 0, "subscription id to deleting")
	return cmd
}

func listSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the subscription ",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			grpcConn := mustGetControllerProxyConn(ctx, cmd)
			defer func() {
				_ = grpcConn.Close()
			}()

			cli := ctrlpb.NewTriggerControllerClient(grpcConn)
			res, err := cli.ListSubscription(ctx, &empty.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list subscription failed: %s", err)
			}
			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(res)
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(getSubscriptionHeader("no."))
				for idx := range res.Subscription {
					sub := res.Subscription[idx]
					t.AppendRow(getSubscriptionRow(sub, idx+1))
					t.AppendSeparator()
				}
				t.SetColumnConfigs(getSubscriptionColumnConfig(
					table.ColumnConfig{Number: 1, VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				))
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
		},
	}
	return cmd
}

var subscriptionHeaders = []string{"id", "eventbus", "sink", "protocol", "sinkCredential", "filter", "transformer", "config", "offsets"}

func getSubscriptionHeader(headers ...interface{}) table.Row {
	for _, k := range subscriptionHeaders {
		headers = append(headers, k)
	}
	return headers
}

func getSubscriptionRow(sub *meta.Subscription, rows ...interface{}) table.Row {
	sinkCredential, _ := json.MarshalIndent(sub.SinkCredential, "", "  ")
	filter, _ := json.MarshalIndent(sub.Filters, "", "  ")
	trans, _ := json.MarshalIndent(sub.Transformer, "", "  ")
	cfg, _ := json.MarshalIndent(sub.Config, "", "  ")
	offsets, _ := json.MarshalIndent(sub.Offsets, "", "  ")
	var protocol string
	switch sub.Protocol {
	case meta.Protocol_HTTP:
		protocol = "http"
	case meta.Protocol_AWS_LAMBDA:
		protocol = "aws-lambda"
	}
	rows = append(rows, sub.Id, sub.EventBus, sub.Sink, protocol, string(sinkCredential), string(filter),
		string(trans), string(cfg), string(offsets))
	return rows
}

func getSubscriptionColumnConfig(columnConfigs ...table.ColumnConfig) []table.ColumnConfig {
	//num := len(columnConfigs)
	for i := 0; i < len(subscriptionHeaders); i++ {
		columnConfigs = append(columnConfigs, table.ColumnConfig{VAlign: text.VAlignMiddle, Align: text.AlignCenter, AlignHeader: text.AlignCenter})
	}
	return columnConfigs
}
