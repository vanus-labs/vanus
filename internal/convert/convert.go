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
	"google.golang.org/protobuf/types/known/structpb"

	ctrl "github.com/vanus-labs/vanus/proto/pkg/controller"
	pb "github.com/vanus-labs/vanus/proto/pkg/meta"
	pbtrigger "github.com/vanus-labs/vanus/proto/pkg/trigger"

	"github.com/vanus-labs/vanus/internal/controller/trigger/metadata"
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
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
		EventbusID:         vanus.NewIDFromUint64(sub.EventbusId),
		Name:               sub.Name,
		Description:        sub.Description,
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
	case pb.Protocol_GCLOUD_FUNCTIONS:
		to = primitive.GCloudFunctions
	case pb.Protocol_GRPC:
		to = primitive.GRPC
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
	case primitive.GCloudFunctions:
		to = pb.Protocol_GCLOUD_FUNCTIONS
	case primitive.GRPC:
		to = pb.Protocol_GRPC
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
	case pb.SinkCredential_AWS:
		to = primitive.AWS
	case pb.SinkCredential_GCLOUD:
		to = primitive.GCloud
	case pb.SinkCredential_PLAIN:
		to = primitive.Plain
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
	case pb.SinkCredential_AWS:
		cloud := from.GetAws()
		return primitive.NewAkSkSinkCredential(cloud.GetAccessKeyId(), cloud.GetSecretAccessKey())
	case pb.SinkCredential_GCLOUD:
		gcloud := from.GetGcloud()
		return primitive.NewGCloudSinkCredential(gcloud.GetCredentialsJson())
	case pb.SinkCredential_PLAIN:
		plain := from.GetPlain()
		return primitive.NewPlainSinkCredential(plain.GetIdentifier(), plain.GetSecret())
	}
	return nil
}

func toPbSinkCredentialByType(credentialType *primitive.CredentialType) *pb.SinkCredential {
	if credentialType == nil {
		return nil
	}
	to := &pb.SinkCredential{}
	switch *credentialType {
	case primitive.AWS:
		to.CredentialType = pb.SinkCredential_AWS
		to.Credential = &pb.SinkCredential_Aws{
			Aws: &pb.AKSKCredential{
				AccessKeyId:     primitive.SecretsMask,
				SecretAccessKey: primitive.SecretsMask,
			},
		}
	case primitive.GCloud:
		to.CredentialType = pb.SinkCredential_GCLOUD
		to.Credential = &pb.SinkCredential_Gcloud{
			Gcloud: &pb.GCloudCredential{
				CredentialsJson: primitive.SecretsMask,
			},
		}
	case primitive.Plain:
		to.CredentialType = pb.SinkCredential_PLAIN
		to.Credential = &pb.SinkCredential_Plain{
			Plain: &pb.PlainCredential{
				Identifier: primitive.SecretsMask,
				Secret:     primitive.SecretsMask,
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
	case primitive.AWS:
		credential, _ := from.(*primitive.AkSkSinkCredential)
		to.CredentialType = pb.SinkCredential_AWS
		to.Credential = &pb.SinkCredential_Aws{
			Aws: &pb.AKSKCredential{
				AccessKeyId:     credential.AccessKeyID,
				SecretAccessKey: credential.SecretAccessKey,
			},
		}
	case primitive.GCloud:
		credential, _ := from.(*primitive.GCloudSinkCredential)
		to.CredentialType = pb.SinkCredential_GCLOUD
		to.Credential = &pb.SinkCredential_Gcloud{
			Gcloud: &pb.GCloudCredential{
				CredentialsJson: credential.CredentialJSON,
			},
		}
	case primitive.Plain:
		credential, _ := from.(*primitive.PlainSinkCredential)
		to.CredentialType = pb.SinkCredential_PLAIN
		to.Credential = &pb.SinkCredential_Plain{
			Plain: &pb.PlainCredential{
				Identifier: credential.Identifier,
				Secret:     credential.Secret,
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
		RateLimit:         config.RateLimit,
		MaxRetryAttempts:  config.MaxRetryAttempts,
		DeliveryTimeout:   config.DeliveryTimeout,
		DisableDeadLetter: config.DisableDeadLetter,
		OrderedEvent:      config.OrderedEvent,
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
		RateLimit:         config.RateLimit,
		MaxRetryAttempts:  config.MaxRetryAttempts,
		DeliveryTimeout:   config.DeliveryTimeout,
		DisableDeadLetter: config.DisableDeadLetter,
		OrderedEvent:      config.OrderedEvent,
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
		ID:                   vanus.ID(sub.Id),
		Sink:                 primitive.URI(sub.Sink),
		SinkCredential:       fromPbSinkCredential(sub.SinkCredential),
		Protocol:             fromPbProtocol(sub.Protocol),
		ProtocolSetting:      fromPbProtocolSettings(sub.ProtocolSettings),
		EventbusID:           vanus.NewIDFromUint64(sub.EventbusId),
		DeadLetterEventbusID: vanus.NewIDFromUint64(sub.DeadLetterEventbusId),
		RetryEventbusID:      vanus.NewIDFromUint64(sub.RetryEventbusId),
		Offsets:              FromPbOffsetInfos(sub.Offsets),
		Filters:              fromPbFilters(sub.Filters),
		Transformer:          fromPbTransformer(sub.Transformer),
		Config:               fromPbSubscriptionConfig(sub.Config),
	}
	return to
}

func ToPbAddSubscription(sub *primitive.Subscription) *pbtrigger.AddSubscriptionRequest {
	to := &pbtrigger.AddSubscriptionRequest{
		Id:                   uint64(sub.ID),
		Sink:                 string(sub.Sink),
		SinkCredential:       toPbSinkCredential(sub.SinkCredential),
		EventbusId:           sub.EventbusID.Uint64(),
		DeadLetterEventbusId: sub.DeadLetterEventbusID.Uint64(),
		RetryEventbusId:      sub.RetryEventbusID.Uint64(),
		Offsets:              ToPbOffsetInfos(sub.Offsets),
		Filters:              toPbFilters(sub.Filters),
		Transformer:          ToPbTransformer(sub.Transformer),
		Config:               toPbSubscriptionConfig(sub.Config),
		Protocol:             toPbProtocol(sub.Protocol),
		ProtocolSettings:     toPbProtocolSettings(sub.ProtocolSetting),
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
		EventbusId:       sub.EventbusID.Uint64(),
		Filters:          toPbFilters(sub.Filters),
		Transformer:      ToPbTransformer(sub.Transformer),
		Offsets:          ToPbOffsetInfos(offsets),
		Name:             sub.Name,
		Description:      sub.Description,
		CreatedAt:        sub.CreatedAt.UnixMilli(),
		UpdatedAt:        sub.UpdatedAt.UnixMilli(),
	}
	if sub.Phase == metadata.SubscriptionPhaseStopped {
		to.Disable = true
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
		EventlogID: vanus.ID(offset.EventlogId),
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
		EventlogId: uint64(offset.EventlogID),
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
		Pipeline: fromPbActions(transformer.Pipeline),
	}
}

func fromPbActions(actions []*pb.Action) []*primitive.Action {
	to := make([]*primitive.Action, 0, len(actions))
	for _, action := range actions {
		to = append(to, fromPbCommand(action))
	}
	return to
}

func fromPbCommand(action *pb.Action) *primitive.Action {
	to := &primitive.Action{}
	commands := make([]interface{}, len(action.Command))
	for i, command := range action.Command {
		commands[i] = command.AsInterface()
	}
	to.Command = commands
	return to
}

func toPbActions(actions []*primitive.Action) []*pb.Action {
	to := make([]*pb.Action, len(actions))
	for i, action := range actions {
		to[i] = toPbCommand(action)
	}
	return to
}

func toPbCommand(action *primitive.Action) *pb.Action {
	to := &pb.Action{}
	commands := make([]*structpb.Value, len(action.Command))
	for i, command := range action.Command {
		c, _ := structpb.NewValue(command)
		commands[i] = c
	}
	to.Command = commands
	return to
}

func ToPbTransformer(transformer *primitive.Transformer) *pb.Transformer {
	if transformer == nil {
		return nil
	}
	return &pb.Transformer{
		Define:   transformer.Define,
		Template: transformer.Template,
		Pipeline: toPbActions(transformer.Pipeline),
	}
}
