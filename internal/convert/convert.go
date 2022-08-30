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

package convert

import (
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	ctrl "github.com/linkall-labs/vanus/proto/pkg/controller"
	pb "github.com/linkall-labs/vanus/proto/pkg/meta"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"
)

func FromPbSubscriptionRequest(sub *ctrl.SubscriptionRequest) *metadata.Subscription {
	to := &metadata.Subscription{
		Source:             sub.Source,
		Types:              sub.Types,
		Config:             fromPbSubscriptionConfig(sub.Config),
		Sink:               primitive.URI(sub.Sink),
		SinkCredential:     fromPbSinkCredential(sub.SinkCredential),
		SinkCredentialType: fromPbSinkCredentialType(sub.SinkCredential),
		Protocol:           fromPbProtocol(sub.Protocol),
		ProtocolSetting:    fromPbProtocolSettings(sub.ProtocolSettings),
		Filters:            fromPbFilters(sub.Filters),
		Transformer:        fromPbTransformer(sub.Transformer),
		EventBus:           sub.EventBus,
	}
	return to
}

func fromPbProtocol(from pb.Protocol) primitive.Protocol {
	var to primitive.Protocol
	switch from {
	case pb.Protocol_HTTP:
		to = primitive.HTTPProtocol
	case pb.Protocol_AWS_LAMBDA:
		to = primitive.AwsLambdaProtocol
	}
	return to
}

func toPbProtocol(from primitive.Protocol) pb.Protocol {
	var to pb.Protocol
	switch from {
	case primitive.HTTPProtocol:
		to = pb.Protocol_HTTP
	case primitive.AwsLambdaProtocol:
		to = pb.Protocol_AWS_LAMBDA
	}
	return to
}

func fromPbProtocolSettings(from *pb.ProtocolSetting) *primitive.ProtocolSetting {
	if from == nil {
		return nil
	}
	to := &primitive.ProtocolSetting{
		Headers: from.Headers,
	}
	return to
}

func toPbProtocolSettings(from *primitive.ProtocolSetting) *pb.ProtocolSetting {
	if from == nil {
		return nil
	}
	to := &pb.ProtocolSetting{
		Headers: from.Headers,
	}
	return to
}

func fromPbSinkCredentialType(from *pb.SinkCredential) *primitive.CredentialType {
	if from == nil {
		return nil
	}
	var to primitive.CredentialType
	switch from.CredentialType {
	case pb.SinkCredential_None:
		return nil
	case pb.SinkCredential_PLAIN:
		to = primitive.Plain
	case pb.SinkCredential_CLOUD:
		to = primitive.Cloud
	}
	return &to
}

func fromPbSinkCredential(from *pb.SinkCredential) primitive.SinkCredential {
	if from == nil {
		return nil
	}
	switch from.CredentialType {
	case pb.SinkCredential_None:
		return nil
	case pb.SinkCredential_PLAIN:
		plain := from.GetPlain()
		return primitive.NewPlainSinkCredential(plain.Identifier, plain.Secret)
	case pb.SinkCredential_CLOUD:
		cloud := from.GetCloud()
		return primitive.NewCloudSinkCredential(cloud.AccessKeyId, cloud.SecretAccessKey)
	}
	return nil
}

func toPbSinkCredentialByType(credentialType *primitive.CredentialType) *pb.SinkCredential {
	if credentialType == nil {
		return nil
	}
	to := &pb.SinkCredential{}
	switch *credentialType {
	case primitive.Plain:
		to.CredentialType = pb.SinkCredential_PLAIN
		to.Credential = &pb.SinkCredential_Plain{
			Plain: &pb.PlainCredential{
				Identifier: primitive.SecretsMask,
				Secret:     primitive.SecretsMask,
			},
		}
	case primitive.Cloud:
		to.CredentialType = pb.SinkCredential_CLOUD
		to.Credential = &pb.SinkCredential_Cloud{
			Cloud: &pb.CloudCredential{
				AccessKeyId:     primitive.SecretsMask,
				SecretAccessKey: primitive.SecretsMask,
			},
		}
	}
	return to
}

func toPbSinkCredential(from primitive.SinkCredential) *pb.SinkCredential {
	if from == nil {
		return nil
	}
	to := &pb.SinkCredential{}
	switch from.GetType() {
	case primitive.Plain:
		credential, _ := from.(*primitive.PlainSinkCredential)
		to.CredentialType = pb.SinkCredential_PLAIN
		to.Credential = &pb.SinkCredential_Plain{
			Plain: &pb.PlainCredential{
				Identifier: credential.Identifier,
				Secret:     credential.Secret,
			},
		}
	case primitive.Cloud:
		credential, _ := from.(*primitive.CloudSinkCredential)
		to.CredentialType = pb.SinkCredential_CLOUD
		to.Credential = &pb.SinkCredential_Cloud{
			Cloud: &pb.CloudCredential{
				AccessKeyId:     credential.AccessKeyID,
				SecretAccessKey: credential.SecretAccessKey,
			},
		}
	}
	return to
}

func fromPbSubscriptionConfig(config *pb.SubscriptionConfig) primitive.SubscriptionConfig {
	if config == nil {
		return primitive.SubscriptionConfig{}
	}

	to := primitive.SubscriptionConfig{
		RateLimit:          config.RateLimit,
		MaxRetryAttempts:   config.MaxRetryAttempts,
		DeliveryTimeout:    config.DeliveryTimeout,
		DeadLetterEventbus: config.DeadLetterEventbus,
	}
	switch config.OffsetType {
	case pb.SubscriptionConfig_LATEST:
		to.OffsetType = primitive.LatestOffset
	case pb.SubscriptionConfig_EARLIEST:
		to.OffsetType = primitive.EarliestOffset
	case pb.SubscriptionConfig_TIMESTAMP:
		to.OffsetType = primitive.Timestamp
		to.OffsetTimestamp = config.OffsetTimestamp
	}
	return to
}

func toPbSubscriptionConfig(config primitive.SubscriptionConfig) *pb.SubscriptionConfig {
	to := &pb.SubscriptionConfig{
		RateLimit:          config.RateLimit,
		MaxRetryAttempts:   config.MaxRetryAttempts,
		DeliveryTimeout:    config.DeliveryTimeout,
		DeadLetterEventbus: config.DeadLetterEventbus,
	}
	switch config.OffsetType {
	case primitive.LatestOffset:
		to.OffsetType = pb.SubscriptionConfig_LATEST
	case primitive.EarliestOffset:
		to.OffsetType = pb.SubscriptionConfig_EARLIEST
	case primitive.Timestamp:
		to.OffsetType = pb.SubscriptionConfig_TIMESTAMP
		to.OffsetTimestamp = config.OffsetTimestamp
	}
	return to
}

func FromPbAddSubscription(sub *pbtrigger.AddSubscriptionRequest) *primitive.Subscription {
	to := &primitive.Subscription{
		ID:              vanus.ID(sub.Id),
		Sink:            primitive.URI(sub.Sink),
		SinkCredential:  fromPbSinkCredential(sub.SinkCredential),
		Protocol:        fromPbProtocol(sub.Protocol),
		ProtocolSetting: fromPbProtocolSettings(sub.ProtocolSettings),
		EventBus:        sub.EventBus,
		Offsets:         FromPbOffsetInfos(sub.Offsets),
		Filters:         fromPbFilters(sub.Filters),
		Transformer:     fromPbTransformer(sub.Transformer),
		Config:          fromPbSubscriptionConfig(sub.Config),
	}
	return to
}

func ToPbAddSubscription(sub *primitive.Subscription) *pbtrigger.AddSubscriptionRequest {
	to := &pbtrigger.AddSubscriptionRequest{
		Id:               uint64(sub.ID),
		Sink:             string(sub.Sink),
		SinkCredential:   toPbSinkCredential(sub.SinkCredential),
		EventBus:         sub.EventBus,
		Offsets:          ToPbOffsetInfos(sub.Offsets),
		Filters:          toPbFilters(sub.Filters),
		Transformer:      toPbTransformer(sub.Transformer),
		Config:           toPbSubscriptionConfig(sub.Config),
		Protocol:         toPbProtocol(sub.Protocol),
		ProtocolSettings: toPbProtocolSettings(sub.ProtocolSetting),
	}
	return to
}

func ToPbSubscription(sub *metadata.Subscription, offsets info.ListOffsetInfo) *pb.Subscription {
	to := &pb.Subscription{
		Id:               uint64(sub.ID),
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           toPbSubscriptionConfig(sub.Config),
		Sink:             string(sub.Sink),
		SinkCredential:   toPbSinkCredentialByType(sub.SinkCredentialType),
		Protocol:         toPbProtocol(sub.Protocol),
		ProtocolSettings: toPbProtocolSettings(sub.ProtocolSetting),
		EventBus:         sub.EventBus,
		Filters:          toPbFilters(sub.Filters),
		Transformer:      toPbTransformer(sub.Transformer),
		Offsets:          ToPbOffsetInfos(offsets),
	}
	return to
}

func fromPbFilters(filters []*pb.Filter) []*primitive.SubscriptionFilter {
	if len(filters) == 0 {
		return nil
	}
	to := make([]*primitive.SubscriptionFilter, 0, len(filters))
	for _, filter := range filters {
		f := fromPbFilter(filter)
		if f == nil {
			continue
		}
		to = append(to, f)
	}
	return to
}

func fromPbFilter(filter *pb.Filter) *primitive.SubscriptionFilter {
	if len(filter.Exact) != 0 {
		return &primitive.SubscriptionFilter{Exact: filter.Exact}
	}
	if len(filter.Suffix) != 0 {
		return &primitive.SubscriptionFilter{Suffix: filter.Suffix}
	}
	if len(filter.Prefix) != 0 {
		return &primitive.SubscriptionFilter{Prefix: filter.Prefix}
	}
	if filter.Not != nil {
		return &primitive.SubscriptionFilter{Not: fromPbFilter(filter.Not)}
	}
	if filter.Sql != "" {
		return &primitive.SubscriptionFilter{CeSQL: filter.Sql}
	}
	if filter.Cel != "" {
		return &primitive.SubscriptionFilter{CEL: filter.Cel}
	}
	if len(filter.All) > 0 {
		return &primitive.SubscriptionFilter{All: fromPbFilters(filter.All)}
	}
	if len(filter.Any) > 0 {
		return &primitive.SubscriptionFilter{Any: fromPbFilters(filter.Any)}
	}
	return nil
}

func toPbFilters(filters []*primitive.SubscriptionFilter) []*pb.Filter {
	to := make([]*pb.Filter, 0, len(filters))
	for _, filter := range filters {
		to = append(to, toPbFilter(filter))
	}
	return to
}

func toPbFilter(filter *primitive.SubscriptionFilter) *pb.Filter {
	if len(filter.Exact) != 0 {
		return &pb.Filter{Exact: filter.Exact}
	}
	if len(filter.Suffix) != 0 {
		return &pb.Filter{Suffix: filter.Suffix}
	}
	if len(filter.Prefix) != 0 {
		return &pb.Filter{Prefix: filter.Prefix}
	}
	if filter.Not != nil {
		return &pb.Filter{Not: toPbFilter(filter.Not)}
	}
	if filter.CeSQL != "" {
		return &pb.Filter{Sql: filter.CeSQL}
	}
	if filter.CEL != "" {
		return &pb.Filter{Cel: filter.CEL}
	}
	if len(filter.All) > 0 {
		return &pb.Filter{All: toPbFilters(filter.All)}
	}
	if len(filter.Any) > 0 {
		return &pb.Filter{Any: toPbFilters(filter.Any)}
	}
	return nil
}

func FromPbOffsetInfos(offsets []*pb.OffsetInfo) info.ListOffsetInfo {
	var to info.ListOffsetInfo
	for _, offset := range offsets {
		to = append(to, fromPbOffsetInfo(offset))
	}
	return to
}

func fromPbOffsetInfo(offset *pb.OffsetInfo) info.OffsetInfo {
	return info.OffsetInfo{
		EventLogID: vanus.ID(offset.EventLogId),
		Offset:     offset.Offset,
	}
}

func ToPbSubscriptionInfo(sub info.SubscriptionInfo) *pb.SubscriptionInfo {
	to := &pb.SubscriptionInfo{
		SubscriptionId: uint64(sub.SubscriptionID),
		Offsets:        ToPbOffsetInfos(sub.Offsets),
	}

	return to
}

func ToPbOffsetInfos(offsets info.ListOffsetInfo) []*pb.OffsetInfo {
	to := make([]*pb.OffsetInfo, 0, len(offsets))
	for _, offset := range offsets {
		to = append(to, toPbOffsetInfo(offset))
	}
	return to
}

func toPbOffsetInfo(offset info.OffsetInfo) *pb.OffsetInfo {
	return &pb.OffsetInfo{
		EventLogId: uint64(offset.EventLogID),
		Offset:     offset.Offset,
	}
}
func fromPbTransformer(transformer *pb.Transformer) *primitive.Transformer {
	if transformer == nil {
		return nil
	}
	return &primitive.Transformer{
		Define:   transformer.Define,
		Template: transformer.Template,
	}
}

func toPbTransformer(transformer *primitive.Transformer) *pb.Transformer {
	if transformer == nil {
		return nil
	}
	return &pb.Transformer{
		Define:   transformer.Define,
		Template: transformer.Template,
	}
}
