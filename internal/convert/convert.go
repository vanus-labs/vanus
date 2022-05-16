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
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	pb "github.com/linkall-labs/vsproto/pkg/meta"
	pbtrigger "github.com/linkall-labs/vsproto/pkg/trigger"
)

func FromPbCreateSubscription(sub *ctrl.CreateSubscriptionRequest) *primitive.SubscriptionData {
	to := &primitive.SubscriptionData{
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          fromPbFilters(sub.Filters),
		InputTransformer: fromPbInputTransformer(sub.InputTransformer),
	}
	return to
}

func FromPbAddSubscription(sub *pbtrigger.AddSubscriptionRequest) *primitive.Subscription {
	to := &primitive.Subscription{
		ID:               vanus.ID(sub.Id),
		Sink:             primitive.URI(sub.Sink),
		EventBus:         sub.EventBus,
		Offsets:          FromPbOffsetInfos(sub.Offsets),
		Filters:          fromPbFilters(sub.Filters),
		InputTransformer: fromPbInputTransformer(sub.InputTransformer),
	}
	return to
}

func ToPbAddSubscription(sub *primitive.Subscription) *pbtrigger.AddSubscriptionRequest {
	to := &pbtrigger.AddSubscriptionRequest{
		Id:               uint64(sub.ID),
		Sink:             string(sub.Sink),
		EventBus:         sub.EventBus,
		Offsets:          ToPbOffsetInfos(sub.Offsets),
		Filters:          toPbFilters(sub.Filters),
		InputTransformer: toPbInputTransformer(sub.InputTransformer),
	}
	return to
}

func FromPbSubscription(sub *pb.Subscription) *primitive.SubscriptionData {
	to := &primitive.SubscriptionData{
		ID:               vanus.ID(sub.Id),
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          fromPbFilters(sub.Filters),
		InputTransformer: fromPbInputTransformer(sub.InputTransformer),
	}
	return to
}

func ToPbSubscription(sub *primitive.SubscriptionData) *pb.Subscription {
	to := &pb.Subscription{
		Id:               uint64(sub.ID),
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             string(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Filters:          toPbFilters(sub.Filters),
		InputTransformer: toPbInputTransformer(sub.InputTransformer),
	}
	return to
}

func fromPbFilters(filters []*pb.Filter) []*primitive.SubscriptionFilter {
	if len(filters) == 0 {
		return nil
	}
	var tos []*primitive.SubscriptionFilter
	for _, filter := range filters {
		f := fromPbFilter(filter)
		if f == nil {
			continue
		}
		tos = append(tos, f)
	}
	return tos
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
	var tos []*pb.Filter
	for _, filter := range filters {
		tos = append(tos, toPbFilter(filter))
	}
	return tos
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
	var to []*pb.OffsetInfo
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
func fromPbInputTransformer(inputTransformer *pb.InputTransformer) *primitive.InputTransformer {
	if inputTransformer == nil {
		return nil
	}
	return &primitive.InputTransformer{
		Define:   inputTransformer.Define,
		Template: inputTransformer.Template,
	}
}

func toPbInputTransformer(inputTransformer *primitive.InputTransformer) *pb.InputTransformer {
	if inputTransformer == nil {
		return nil
	}
	return &pb.InputTransformer{
		Define:   inputTransformer.Define,
		Template: inputTransformer.Template,
	}
}
