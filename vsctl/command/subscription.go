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
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/fatih/color"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/linkall-labs/vanus/internal/convert"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/meta"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"github.com/spf13/cobra"
	"k8s.io/utils/strings/slices"
)

func NewSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "subscription sub-command ",
		Short: "sub-commands for subscription operations",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			InitGatewayClient(cmd)
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			DestroyGatewayClient()
		},
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
				if sinkCredentialType != AWSCredentialType {
					cmdFailedf(cmd, "protocol is aws-lambda, credential-type must be %s\n", AWSCredentialType)
				}
			case "gcloud-functions":
				p = meta.Protocol_GCLOUD_FUNCTIONS
				if sinkCredentialType != GCloudCredentialType {
					cmdFailedf(cmd, "protocol is aws-lambda, credential-type must be %s\n", GCloudCredentialType)
				}
			default:
				cmdFailedf(cmd, "protocol is invalid\n")
			}

			var credential *meta.SinkCredential
			if sinkCredentialType != "" {
				if sinkCredential == "" {
					cmdFailedf(cmd, "credential-type is set but sinkCredential empty\n")
				}
				if sinkCredential[0] == '@' {
					credentialBytes, err := ioutil.ReadFile(sinkCredential[1:])
					if err != nil {
						cmdFailedf(cmd, "read sinkCredential file:%s error:%s\n", sinkCredential, err.Error())
					}
					sinkCredential = string(credentialBytes)
					fmt.Println(sinkCredential)
				}
				// expand value from env
				sinkCredential = os.ExpandEnv(sinkCredential)
				switch sinkCredentialType {
				case AWSCredentialType:
					var akSK *meta.AKSKCredential
					err := json.Unmarshal([]byte(sinkCredential), &akSK)
					if err != nil {
						cmdFailedf(cmd, "the sink credential unmarshal json error: %s", err.Error())
					}
					if akSK.AccessKeyId == "" || akSK.SecretAccessKey == "" {
						cmdFailedf(cmd, "credential-type is aws, access_key_id and secret_access_key must not be empty\n")
					}
					credential = &meta.SinkCredential{
						CredentialType: meta.SinkCredential_AWS,
						Credential: &meta.SinkCredential_Aws{
							Aws: akSK,
						},
					}
				case GCloudCredentialType:
					var m map[string]string
					err := json.Unmarshal([]byte(sinkCredential), &m)
					if err != nil {
						cmdFailedf(cmd, "the sink credential unmarshal json error: %s", err.Error())
					}
					credential = &meta.SinkCredential{
						CredentialType: meta.SinkCredential_GCLOUD,
						Credential: &meta.SinkCredential_Gcloud{
							Gcloud: &meta.GCloudCredential{
								CredentialsJson: sinkCredential,
							},
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
				var _transformer *primitive.Transformer
				err := json.Unmarshal([]byte(transformer), &_transformer)
				if err != nil {
					cmdFailedf(cmd, "the transformer invalid: %s", err)
				}
				trans = convert.ToPbTransformer(_transformer)
			}

			// subscription config
			config := &meta.SubscriptionConfig{
				RateLimit:       rateLimit,
				DeliveryTimeout: deliveryTimeout,
				OrderedEvent:    orderedPushEvent,
			}
			if maxRetryAttempts >= 0 {
				value := uint32(maxRetryAttempts)
				config.MaxRetryAttempts = &value
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

			res, err := client.CreateSubscription(context.Background(), &ctrlpb.CreateSubscriptionRequest{
				Subscription: &ctrlpb.SubscriptionRequest{
					Source:         source,
					Config:         config,
					Filters:        filter,
					Sink:           sink,
					SinkCredential: credential,
					Protocol:       p,
					EventBus:       eventbus,
					Transformer:    trans,
					Name:           subscriptionName,
					Description:    description,
					Disable:        disableSubscription,
				},
			})
			if err != nil {
				cmdFailedf(cmd, "create subscription failed: %s", err)
			}
			printSubscription(cmd, false, false, false, res)
		},
	}
	cmd.Flags().StringVar(&eventbus, "eventbus", "", "eventbus name to consuming")
	cmd.Flags().StringVar(&sink, "sink", "", "the event you want to send to")
	cmd.Flags().StringVar(&filters, "filters", "", "filter event you interested, JSON format required")
	cmd.Flags().StringVar(&transformer, "transformer", "", "transformer, JSON format required")
	cmd.Flags().Uint32Var(&rateLimit, "rate-limit", 0, "max event number pushing to sink per second, default is 0, means unlimited")
	cmd.Flags().StringVar(&from, "from", "", "consume events from, latest,earliest or RFC3339 format time")
	cmd.Flags().StringVar(&subProtocol, "protocol", "http", "protocol,http or aws-lambda or gcloud-functions")
	cmd.Flags().StringVar(&sinkCredentialType, "credential-type", "", "sink credential type: aws or gcloud")
	cmd.Flags().StringVar(&sinkCredential, "credential", "", "sink credential info, JSON format or @file")
	cmd.Flags().Uint32Var(&deliveryTimeout, "delivery-timeout", 0, "event delivery to sink timeout by millisecond, default is 0, means using server-side default value: 5s")
	cmd.Flags().Int32Var(&maxRetryAttempts, "max-retry-attempts", -1, "event delivery fail max retry attempts, default is -1, means using server-side max retry attempts: 32")
	cmd.Flags().StringVar(&subscriptionName, "name", "", "subscription name")
	cmd.Flags().StringVar(&description, "description", "", "subscription description")
	cmd.Flags().BoolVar(&disableSubscription, "disable", false, "whether disable the "+
		"subscription (just create if disable=true)")
	cmd.Flags().BoolVar(&orderedPushEvent, "ordered-event", false, "whether push the "+
		"event with ordered")
	return cmd
}

func deleteSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "delete a subscription",
		Run: func(cmd *cobra.Command, args []string) {
			id, err := vanus.NewIDFromString(subscriptionIDStr)
			if err != nil {
				cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid subscription id: %s\n", err.Error()))
			}

			_, err = client.DeleteSubscription(context.Background(), &ctrlpb.DeleteSubscriptionRequest{
				Id: id.Uint64(),
			})
			if err != nil {
				cmdFailedf(cmd, "delete subscription failed: %s", err)
			}

			if IsFormatJSON(cmd) {
				data, _ := json.Marshal(map[string]interface{}{"subscription_id": subscriptionIDStr})
				color.Green(string(data))
			} else {
				t := table.NewWriter()
				t.AppendHeader(table.Row{"subscription_id"})
				t.AppendRow(table.Row{subscriptionIDStr})
				t.SetColumnConfigs([]table.ColumnConfig{
					{Number: 1, Align: text.AlignCenter, AlignHeader: text.AlignCenter},
				})
				t.SetOutputMirror(os.Stdout)
				t.Render()
			}
			color.Green("delete subscription: %d success\n", subscriptionIDStr)
		},
	}
	cmd.Flags().StringVar(&subscriptionIDStr, "id", "", "subscription id to deleting")
	return cmd
}

func getSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "info",
		Short: "get the subscription info ",
		Run: func(cmd *cobra.Command, args []string) {
			id, err := vanus.NewIDFromString(subscriptionIDStr)
			if err != nil {
				cmdFailedWithHelpNotice(cmd, fmt.Sprintf("invalid subscription id: %s\n", err.Error()))
			}

			res, err := client.GetSubscription(context.Background(), &ctrlpb.GetSubscriptionRequest{
				Id: id.Uint64(),
			})
			if err != nil {
				cmdFailedf(cmd, "get subscription info failed: %s", err)
			}
			printSubscription(cmd, false, true, true, res)
		},
	}
	cmd.Flags().StringVar(&subscriptionIDStr, "id", "", "subscription id")
	return cmd
}

func listSubscriptionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "list the subscription ",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := client.ListSubscription(context.Background(), &empty.Empty{})
			if err != nil {
				cmdFailedf(cmd, "list subscription failed: %s", err)
			}
			printSubscription(cmd, true, false, false, res.Subscription...)
		},
	}
	return cmd
}

func printSubscription(cmd *cobra.Command, showNo, showFilters, showTransformer bool, data ...*metapb.Subscription) {
	if IsFormatJSON(cmd) {
		data, _ := json.Marshal(data)
		color.Green(string(data))
	} else {
		t := table.NewWriter()
		header := getSubscriptionHeader(showNo)
		t.AppendHeader(header)
		for idx := range data {
			var row []interface{}
			if showNo {
				row = append(row, idx+1)
			}
			sub := data[idx]

			row = append(row, getSubscriptionRow(sub)...)

			filter, _ := json.MarshalIndent(sub.Filters, "", "  ")
			if !showFilters && len(filter) > 10 {
				filter = []byte("...")
			}
			row = append(row, string(filter))

			trans, _ := json.MarshalIndent(sub.Transformer, "", "  ")
			if !showFilters && len(trans) > 10 {
				trans = []byte("...")
			}
			row = append(row, string(trans))
			row = append(row, time.UnixMilli(sub.CreatedAt).Format(time.RFC3339))
			row = append(row, time.UnixMilli(sub.UpdatedAt).Format(time.RFC3339))
			t.AppendRow(row)
			t.AppendSeparator()
		}
		t.SetColumnConfigs(getSubscriptionColumnConfig(header))
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}
}

var subscriptionHeaders = []interface{}{"id", "name", "disable", "eventbus", "sink", "description", "protocol", "sinkCredential",
	"config", "offsets", "filter", "transformer", "created_at", "updated_at"}

func getSubscriptionHeader(showNo bool) table.Row {
	var result []interface{}
	if showNo {
		result = append(result, "no.")
	}
	result = append(result, subscriptionHeaders...)
	return result
}

func getSubscriptionRow(sub *meta.Subscription) []interface{} {
	var result []interface{}
	result = append(result, formatID(sub.Id))
	result = append(result, sub.Name)
	result = append(result, sub.Disable)
	result = append(result, sub.EventBus)
	result = append(result, sub.Sink)
	result = append(result, sub.Description)

	var protocol string
	switch sub.Protocol {
	case meta.Protocol_HTTP:
		protocol = "http"
	case meta.Protocol_AWS_LAMBDA:
		protocol = "aws-lambda"
	case meta.Protocol_GCLOUD_FUNCTIONS:
		protocol = "gcloud-functions"
	}
	result = append(result, protocol)

	sinkCredential, _ := json.MarshalIndent(sub.SinkCredential, "", "  ")
	result = append(result, string(sinkCredential))

	cfg, _ := json.MarshalIndent(sub.Config, "", "  ")
	result = append(result, string(cfg))

	offsets, _ := json.MarshalIndent(sub.Offsets, "", "  ")
	result = append(result, string(offsets))
	return result
}

func getSubscriptionColumnConfig(header table.Row) []table.ColumnConfig {
	var columnConfigs []table.ColumnConfig
	for i := 0; i < len(header); i++ {
		if slices.Contains([]string{"filter", "transformer"}, header[i].(string)) {
			columnConfigs = append(columnConfigs, table.ColumnConfig{
				Number:      i + 1,
				VAlign:      text.VAlignMiddle,
				AlignHeader: text.AlignCenter,
			})
		} else {
			columnConfigs = append(columnConfigs, table.ColumnConfig{
				Number:      i + 1,
				VAlign:      text.VAlignMiddle,
				Align:       text.AlignCenter,
				AlignHeader: text.AlignCenter,
			})
		}
	}
	return columnConfigs
}
