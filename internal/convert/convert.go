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
	ctrl "github.com/linkall-labs/vsproto/pkg/controller"
	pb "github.com/linkall-labs/vsproto/pkg/meta"
	pbtrigger "github.com/linkall-labs/vsproto/pkg/trigger"
)

func FromPbCreateSubscription(sub *ctrl.CreateSubscriptionRequest) *primitive.Subscription {
	to := &primitive.Subscription{
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
	}
	if len(sub.Filters) != 0 {
		to.Filters = fromPbFilters(sub.Filters)
	}
	return to
}

func FromPbAddSubscription(sub *pbtrigger.AddSubscriptionRequest) *primitive.SubscriptionInfo {
	to := &primitive.SubscriptionInfo{
		Subscription: primitive.Subscription{
			Source:           sub.Source,
			Types:            sub.Types,
			Config:           sub.Config,
			Sink:             primitive.URI(sub.Sink),
			Protocol:         sub.Protocol,
			ProtocolSettings: sub.ProtocolSettings,
			EventBus:         sub.EventBus,
		},
		Offsets: FromOffsetInfos(sub.Offsets...),
	}
	if len(sub.Filters) != 0 {
		to.Filters = fromPbFilters(sub.Filters)
	}
	return to
}

func ToPbAddSubscription(sub *primitive.SubscriptionInfo) *pbtrigger.AddSubscriptionRequest {
	to := &pbtrigger.AddSubscriptionRequest{
		Id:               sub.ID,
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             string(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
		Offsets:          ToOffsetInfos(sub.Offsets...),
	}
	if len(sub.Filters) != 0 {
		to.Filters = toPbFilters(sub.Filters)
	}
	return to
}

func FromPbSubscription(sub *pb.Subscription) *primitive.Subscription {
	to := &primitive.Subscription{
		ID:               sub.Id,
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             primitive.URI(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
	}
	if len(sub.Filters) != 0 {
		to.Filters = fromPbFilters(sub.Filters)
	}
	return to
}

func ToPbSubscription(sub *primitive.Subscription) *pb.Subscription {
	to := &pb.Subscription{
		Id:               sub.ID,
		Source:           sub.Source,
		Types:            sub.Types,
		Config:           sub.Config,
		Sink:             string(sub.Sink),
		Protocol:         sub.Protocol,
		ProtocolSettings: sub.ProtocolSettings,
		EventBus:         sub.EventBus,
	}
	if len(sub.Filters) != 0 {
		to.Filters = toPbFilters(sub.Filters)
	}
	return to
}

func fromPbFilters(filters []*pb.Filter) []*primitive.SubscriptionFilter {
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
	if len(filter.All) > 0 {
		return &pb.Filter{All: toPbFilters(filter.All)}
	}
	if len(filter.Any) > 0 {
		return &pb.Filter{Any: toPbFilters(filter.Any)}
	}
	return nil
}

func FromPbSubscriptionInfo(sub *pb.SubscriptionInfo) *info.SubscriptionInfo {
	to := &info.SubscriptionInfo{
		SubId:   sub.SubscriptionId,
		Offsets: FromOffsetInfos(sub.Offsets...),
	}
	return to
}

func FromOffsetInfos(offsets ...*pb.OffsetInfo) []info.OffsetInfo {
	to := make([]info.OffsetInfo, len(offsets))
	for _, offset := range offsets {
		to = append(to, FromOffsetInfo(offset))
	}
	return to
}

func FromOffsetInfo(offset *pb.OffsetInfo) info.OffsetInfo {
	return info.OffsetInfo{
		EventLog: offset.EventLog,
		Offset:   offset.Offset,
	}
}

func ToPbSubscriptionInfo(sub info.SubscriptionInfo) *pb.SubscriptionInfo {
	to := &pb.SubscriptionInfo{
		SubscriptionId: sub.SubId,
		Offsets:        ToOffsetInfos(sub.Offsets...),
	}

	return to
}

func ToOffsetInfos(offsets ...info.OffsetInfo) []*pb.OffsetInfo {
	to := make([]*pb.OffsetInfo, len(offsets))
	for _, offset := range offsets {
		to = append(to, ToOffsetInfo(offset))
	}
	return to
}

func ToOffsetInfo(offset info.OffsetInfo) *pb.OffsetInfo {
	return &pb.OffsetInfo{
		EventLog: offset.EventLog,
		Offset:   offset.Offset,
	}
}
