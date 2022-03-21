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

package filter

import (
	"context"
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
)

func extractFilter(subscriptionFilter *primitive.SubscriptionFilter) Filter {
	ctx := context.Background()
	if len(subscriptionFilter.Exact) > 0 {
		return NewExactFilter(subscriptionFilter.Exact)
	}
	if len(subscriptionFilter.Prefix) > 0 {
		return NewPrefixFilter(subscriptionFilter.Prefix)
	}
	if len(subscriptionFilter.Suffix) > 0 {
		return NewSuffixFilter(subscriptionFilter.Suffix)
	}
	if subscriptionFilter.Not != nil {
		f := NewNotFilter(extractFilter(subscriptionFilter.Not))
		if f == nil {
			log.Debug(ctx, "new not filter is nil ", map[string]interface{}{"filter": subscriptionFilter.Not})
		}
		return f
	}
	if subscriptionFilter.SQL != "" {
		f := NewCESQLFilter(subscriptionFilter.SQL)
		if f == nil {
			log.Debug(ctx, "new cesql filter is nil ", map[string]interface{}{"sql": subscriptionFilter.SQL})
		}
		return f
	}
	if len(subscriptionFilter.All) > 0 {
		f := NewAllFilter(extractFilters(subscriptionFilter.All)...)
		if f == nil {
			log.Debug(ctx, "new all filter is nil ", map[string]interface{}{"filters": subscriptionFilter.All})
		}
		return f
	}
	if len(subscriptionFilter.Any) > 0 {
		f := NewAnyFilter(extractFilters(subscriptionFilter.Any)...)
		if f == nil {
			log.Debug(ctx, "new any filter is nil ", map[string]interface{}{"filters": subscriptionFilter.Any})
		}
		return f
	}
	return nil
}

func extractFilters(subscriptionFilters []*primitive.SubscriptionFilter) []Filter {
	var filters []Filter
	for _, subscriptionFilter := range subscriptionFilters {
		tf := extractFilter(subscriptionFilter)
		if tf == nil {
			log.Debug(context.Background(), "get filter is nil will ignore the filter", map[string]interface{}{"filter": subscriptionFilter})
			continue
		}
		filters = append(filters, tf)
	}
	return filters
}

func GetFilter(subscriptionFilters []*primitive.SubscriptionFilter) Filter {
	filters := extractFilters(subscriptionFilters)
	if len(filters) == 0 {
		return NewNoFilter()
	}
	if len(filters) == 1 {
		return filters[0]
	}
	return NewAllFilter(filters...)
}

func FilterEvent(f Filter, event ce.Event) FilterResult {
	if f == nil {
		return PassFilter
	}
	return f.Filter(event)
}
