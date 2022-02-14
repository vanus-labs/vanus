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

package primitive

import (
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
)

type TriggerFilter struct {
	All    []TriggerFilter   `json:"all,omitempty"`
	Any    []TriggerFilter   `json:"any,omitempty"`
	Not    *TriggerFilter    `json:"not,omitempty"`
	Exact  map[string]string `json:"exact,omitempty"`
	Prefix map[string]string `json:"prefix,omitempty"`
	Suffix map[string]string `json:"suffix,omitempty"`
	SQL    string            `json:"sql,omitempty"`
}

func (triggerFilter TriggerFilter) getFilter() filter.Filter {
	if len(triggerFilter.Exact) > 0 {
		for attribute, value := range triggerFilter.Exact {
			return filter.NewExactFilter(attribute, value)
		}
	}
	if len(triggerFilter.Prefix) > 0 {
		for attribute, prefix := range triggerFilter.Prefix {
			return filter.NewPrefixFilter(attribute, prefix)
		}
	}
	if len(triggerFilter.Suffix) > 0 {
		for attribute, suffix := range triggerFilter.Suffix {
			return filter.NewSuffixFilter(attribute, suffix)
		}
	}
	if len(triggerFilter.All) > 0 {
		triggerFilters := TriggerFilters(triggerFilter.All)
		return filter.NewAllFilter(triggerFilters.getFilters()...)
	}
	if len(triggerFilter.Any) > 0 {
		triggerFilters := TriggerFilters(triggerFilter.All)
		return filter.NewAnyFilter(triggerFilters.getFilters()...)
	}
	if triggerFilter.Not != nil {
		return filter.NewNotFilter(triggerFilter.Not.getFilter())
	}
	if triggerFilter.SQL != "" {
		return filter.NewCESQLFilter(triggerFilter.SQL)
	}
	return nil
}

type TriggerFilters []TriggerFilter

func (triggerFilters TriggerFilters) getFilters() []filter.Filter {
	filters := make([]filter.Filter, len(triggerFilters))
	for _, triggerFilter := range triggerFilters {
		filters = append(filters, triggerFilter.getFilter())
	}
	return filters
}

func (triggerFilters TriggerFilters) FilterEvent(event cloudevents.Event) filter.FilterResult {
	return filter.NewAllFilter(triggerFilters.getFilters()...).Filter(event)
}
