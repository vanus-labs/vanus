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
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/observability/log"
)

type anyFilter []Filter

// NewAnyFilter returns an event filter which passes if all the contained filters pass
func NewAnyFilter(filters ...Filter) Filter {
	return append(anyFilter{}, filters...)
}

func (filter anyFilter) Filter(event cloudevents.Event) FilterResult {
	res := NoFilter
	log.Debug("any filter ", map[string]interface{}{"filters": filter, "event": event})
	for _, f := range filter {
		res = res.Or(f.Filter(event))
		// Short circuit to optimize it
		if res == PassFilter {
			return PassFilter
		}
	}
	return res
}

var _ Filter = (*anyFilter)(nil)
