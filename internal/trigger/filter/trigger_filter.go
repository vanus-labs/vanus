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
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/observability/log"
)

func extractFilter(subscriptionFilter primitive.SubscriptionFilter) Filter {
	if len(subscriptionFilter.Exact) > 0 {
		for attribute, value := range subscriptionFilter.Exact {
			f := NewExactFilter(attribute, value)
			if f == nil {
				log.Debug("new exact filter is nil ", map[string]interface{}{"attribute": attribute, "value": value})
			}
			return f
		}
	}
	if len(subscriptionFilter.Prefix) > 0 {
		for attribute, prefix := range subscriptionFilter.Prefix {
			f := NewPrefixFilter(attribute, prefix)
			if f == nil {
				log.Debug("new prefix filter is nil ", map[string]interface{}{"attribute": attribute, "prefix": prefix})
			}
			return f
		}
	}
	if len(subscriptionFilter.Suffix) > 0 {
		for attribute, suffix := range subscriptionFilter.Suffix {
			f := NewSuffixFilter(attribute, suffix)
			if f == nil {
				log.Debug("new suffix filter is nil ", map[string]interface{}{"attribute": attribute, "suffix": suffix})
			}
			return f
		}
	}
	if subscriptionFilter.Not != nil {
		f := NewNotFilter(extractFilter(*subscriptionFilter.Not))
		if f == nil {
			log.Debug("new not filter is nil ", map[string]interface{}{"filter": subscriptionFilter.Not})
		}
		return f
	}
	if subscriptionFilter.SQL != "" {
		f := NewCESQLFilter(subscriptionFilter.SQL)
		if f == nil {
			log.Debug("new cesql filter is nil ", map[string]interface{}{"sql": subscriptionFilter.SQL})
		}
		return f
	}
	if len(subscriptionFilter.All) > 0 {
		f := NewAllFilter(extractFilters(subscriptionFilter.All)...)
		if f == nil {
			log.Debug("new all filter is nil ", map[string]interface{}{"filters": subscriptionFilter.All})
		}
		return f
	}
	if len(subscriptionFilter.Any) > 0 {
		f := NewAnyFilter(extractFilters(subscriptionFilter.Any)...)
		if f == nil {
			log.Debug("new any filter is nil ", map[string]interface{}{"filters": subscriptionFilter.Any})
		}
		return f
	}
	return nil
}

func extractFilters(subscriptionFilters []primitive.SubscriptionFilter) []Filter {
	var filters []Filter
	for _, subscriptionFilter := range subscriptionFilters {
		tf := extractFilter(subscriptionFilter)
		if tf == nil {
			log.Debug("get filter is nil will ignore the filter", map[string]interface{}{"filter": subscriptionFilter})
			continue
		}
		filters = append(filters, tf)
	}
	return filters
}

func GetFilter(subscriptionFilters []primitive.SubscriptionFilter) Filter {
	return NewAllFilter(extractFilters(subscriptionFilters)...)
}

func FilterEvent(f Filter, event ce.Event) FilterResult {
	if f == nil {
		return PassFilter
	}
	return f.Filter(event)
}
