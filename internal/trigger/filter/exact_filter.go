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

type exactFilter struct {
	attribute, value string
}

// NewExactFilter returns an event filter which passes if value exactly matches the value of the context
// attribute in the CloudEvent.
func NewExactFilter(attribute, value string) Filter {
	return &exactFilter{attribute: attribute, value: value}
}

func (filter *exactFilter) Filter(event cloudevents.Event) FilterResult {
	if filter == nil || filter.attribute == "" || filter.value == "" {
		return NoFilter
	}
	log.Debug("exact filter ", map[string]interface{}{"filters": filter, "event": event})
	value, ok := LookupAttribute(event, filter.attribute)
	if !ok || value != filter.value {
		return FailFilter
	}
	return PassFilter
}

var _ Filter = (*exactFilter)(nil)
