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

package eventfilter

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type exactFilter struct {
	attribute, value string
}

// NewExactFilter returns an event filter which passes if value exactly matches the value of the context
// attribute in the CloudEvent.
func NewExactFilter(attribute, value string) (Filter, error) {
	if attribute == "" || value == "" {
		return nil, fmt.Errorf("invalid arguments, attribute and value can't be empty")
	}
	return &exactFilter{
		attribute: attribute,
		value:     value,
	}, nil
}

func (filter *exactFilter) Filter(ctx context.Context, event cloudevents.Event) FilterResult {
	if filter == nil || filter.attribute == "" || filter.value == "" {
		return NoFilter
	}
	value, ok := LookupAttribute(event, filter.attribute)
	if !ok || value != filter.value {
		return FailFilter
	}
	return PassFilter
}
