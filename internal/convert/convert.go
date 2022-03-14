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
	pb "github.com/linkall-labs/vsproto/pkg/meta"
)

func FromPbSubscription(sub *pb.Subscription) (*primitive.Subscription, error) {
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
	return to, nil
}

func ToPbSubscription(sub *primitive.Subscription) (*pb.Subscription, error) {
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
	return to, nil
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
		return &primitive.SubscriptionFilter{SQL: filter.Sql}
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
	if filter.SQL != "" {
		return &pb.Filter{Sql: filter.SQL}
	}
	if len(filter.All) > 0 {
		return &pb.Filter{All: toPbFilters(filter.All)}
	}
	if len(filter.Any) > 0 {
		return &pb.Filter{Any: toPbFilters(filter.Any)}
	}
	return nil
}
