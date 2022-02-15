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
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/trigger/filter"
	"github.com/linkall-labs/vanus/observability/log"
)

type TriggerFilter struct {
	Exact  map[string]string `json:"exact,omitempty"`
	Prefix map[string]string `json:"prefix,omitempty"`
	Suffix map[string]string `json:"suffix,omitempty"`
	SQL    string            `json:"sql,omitempty"`
	Not    *TriggerFilter    `json:"not,omitempty"`
	All    []TriggerFilter   `json:"all,omitempty"`
	Any    []TriggerFilter   `json:"any,omitempty"`
}

func (triggerFilter TriggerFilter) getFilter() filter.Filter {
	if len(triggerFilter.Exact) > 0 {
		for attribute, value := range triggerFilter.Exact {
			f := filter.NewExactFilter(attribute, value)
			if f == nil {
				log.Debug("new exact filter is nil ", map[string]interface{}{"attribute": attribute, "value": value})
			}
			return f
		}
	}
	if len(triggerFilter.Prefix) > 0 {
		for attribute, prefix := range triggerFilter.Prefix {
			f := filter.NewPrefixFilter(attribute, prefix)
			if f == nil {
				log.Debug("new prefix filter is nil ", map[string]interface{}{"attribute": attribute, "prefix": prefix})
			}
			return f
		}
	}
	if len(triggerFilter.Suffix) > 0 {
		for attribute, suffix := range triggerFilter.Suffix {
			f := filter.NewSuffixFilter(attribute, suffix)
			if f == nil {
				log.Debug("new suffix filter is nil ", map[string]interface{}{"attribute": attribute, "suffix": suffix})
			}
			return f
		}
	}
	if triggerFilter.Not != nil {
		f := filter.NewNotFilter(triggerFilter.Not.getFilter())
		if f == nil {
			log.Debug("new not filter is nil ", map[string]interface{}{"filter": triggerFilter.Not})
		}
		return f
	}
	if triggerFilter.SQL != "" {
		f := filter.NewCESQLFilter(triggerFilter.SQL)
		if f == nil {
			log.Debug("new cesql filter is nil ", map[string]interface{}{"sql": triggerFilter.SQL})
		}
		return f
	}
	if len(triggerFilter.All) > 0 {
		f := filter.NewAllFilter(TriggerFilters(triggerFilter.All).getFilters()...)
		if f == nil {
			log.Debug("new all filter is nil ", map[string]interface{}{"filters": triggerFilter.All})
		}
		return f
	}
	if len(triggerFilter.Any) > 0 {
		f := filter.NewAnyFilter(TriggerFilters(triggerFilter.Any).getFilters()...)
		if f == nil {
			log.Debug("new any filter is nil ", map[string]interface{}{"filters": triggerFilter.Any})
		}
		return f
	}
	return nil
}

type TriggerFilters []TriggerFilter

func (triggerFilters TriggerFilters) getFilters() []filter.Filter {
	var filters []filter.Filter
	for _, triggerFilter := range triggerFilters {
		tf := triggerFilter.getFilter()
		if tf == nil {
			log.Debug("get filter is nil will ignore the filter", map[string]interface{}{"filter": triggerFilter})
			continue
		}
		filters = append(filters, tf)
	}
	return filters
}

func (triggerFilters TriggerFilters) GetFilter() filter.Filter {
	return filter.NewAllFilter(triggerFilters.getFilters()...)
}

func (triggerFilters TriggerFilters) FilterEvent(event ce.Event) filter.FilterResult {
	return triggerFilters.GetFilter().Filter(event)
}

func FilterEvent(f filter.Filter, event ce.Event) filter.FilterResult {
	return f.Filter(event)
}
