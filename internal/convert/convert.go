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

func FromPbCreateSubscription(sub *ctrl.CreateSubscriptionRequest) *metadata.Subscription {
	to := &metadata.Subscription{
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           FromPbSubscriptionConfig(sub.Config),
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          FromPbFilters(sub.Filters),
		Transformer:      FromPbTransformer(sub.Transformer),
	}
	return to
}

func FromPbSubscriptionConfig(config *pb.SubscriptionConfig) primitive.SubscriptionConfig {
	if config == nil {
		return primitive.SubscriptionConfig{}
	}

	to := primitive.SubscriptionConfig{
		RateLimit: config.RateLimit,
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
		RateLimit: config.RateLimit,
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
		ID:          vanus.ID(sub.Id),
		Sink:        primitive.URI(sub.Sink),
		EventBus:    sub.EventBus,
		Offsets:     FromPbOffsetInfos(sub.Offsets),
		Filters:     FromPbFilters(sub.Filters),
		Transformer: FromPbTransformer(sub.Transformer),
		Config:      FromPbSubscriptionConfig(sub.Config),
	}
	return to
}

func ToPbAddSubscription(sub *primitive.Subscription) *pbtrigger.AddSubscriptionRequest {
	to := &pbtrigger.AddSubscriptionRequest{
		Id:          uint64(sub.ID),
		Sink:        string(sub.Sink),
		EventBus:    sub.EventBus,
		Offsets:     ToPbOffsetInfos(sub.Offsets),
		Filters:     toPbFilters(sub.Filters),
		Transformer: toPbTransformer(sub.Transformer),
		Config:      toPbSubscriptionConfig(sub.Config),
	}
	return to
}

func FromPbSubscription(sub *pb.Subscription) *metadata.Subscription {
	to := &metadata.Subscription{
		ID:               vanus.ID(sub.Id),
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           FromPbSubscriptionConfig(sub.Config),
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          FromPbFilters(sub.Filters),
		Transformer:      FromPbTransformer(sub.Transformer),
	}
	return to
}

func ToPbSubscription(sub *metadata.Subscription) *pb.Subscription {
	to := &pb.Subscription{
		Id:               uint64(sub.ID),
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           toPbSubscriptionConfig(sub.Config),
		Sink:             string(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          toPbFilters(sub.Filters),
		Transformer:      toPbTransformer(sub.Transformer),
	}
	return to
}

func FromPbFilters(filters []*pb.Filter) []*primitive.SubscriptionFilter {
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
		return &primitive.SubscriptionFilter{All: FromPbFilters(filter.All)}
	}
	if len(filter.Any) > 0 {
		return &primitive.SubscriptionFilter{Any: FromPbFilters(filter.Any)}
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
		to = append(to, FromPbOffsetInfo(offset))
	}
	return to
}

func FromPbOffsetInfo(offset *pb.OffsetInfo) info.OffsetInfo {
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
func FromPbTransformer(transformer *pb.Transformer) *primitive.Transformer {
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
