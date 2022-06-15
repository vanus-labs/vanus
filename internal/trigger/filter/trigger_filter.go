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

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"

	ce "github.com/cloudevents/sdk-go/v2"
)

func extractFilter(subscriptionFilter *primitive.SubscriptionFilter) Filter {
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
		return NewNotFilter(extractFilter(subscriptionFilter.Not))
	}
	if subscriptionFilter.CeSQL != "" {
		return NewCESQLFilter(subscriptionFilter.CeSQL)
	}
	if subscriptionFilter.CEL != "" {
		return NewCELFilter(subscriptionFilter.CEL)
	}
	if len(subscriptionFilter.All) > 0 {
		return NewAllFilter(extractFilters(subscriptionFilter.All)...)
	}
	if len(subscriptionFilter.Any) > 0 {
		return NewAnyFilter(extractFilters(subscriptionFilter.Any)...)
	}
	return nil
}

func extractFilters(subscriptionFilters []*primitive.SubscriptionFilter) []Filter {
	filters := make([]Filter, 0)
	for _, subscriptionFilter := range subscriptionFilters {
		tf := extractFilter(subscriptionFilter)
		if tf == nil {
			log.Info(context.Background(), "get filter is nil will ignore the filter", map[string]interface{}{
				"filter": subscriptionFilter,
			})
			continue
		}
		filters = append(filters, tf)
	}
	return filters
}

func GetFilter(subscriptionFilters []*primitive.SubscriptionFilter) Filter {
	filters := extractFilters(subscriptionFilters)
	if len(filters) == 0 {
		return nil
	}
	if len(filters) == 1 {
		return filters[0]
	}
	return NewAllFilter(filters...)
}

func Run(f Filter, event ce.Event) Result {
	if f == nil {
		return PassFilter
	}
	return f.Filter(event)
}
