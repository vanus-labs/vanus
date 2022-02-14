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
	"strings"
)

type prefixFilter struct {
	attribute, prefix string
}

// NewPrefixFilter returns an event filter which passes if the value of the context
// attribute in the CloudEvent is prefixed with value.
func NewPrefixFilter(attribute, prefix string) (Filter, error) {
	if attribute == "" || prefix == "" {
		return nil, fmt.Errorf("invalid arguments, attribute and value can't be empty")
	}
	return &prefixFilter{
		attribute: attribute,
		prefix:    prefix,
	}, nil
}

func (filter *prefixFilter) Filter(ctx context.Context, event cloudevents.Event) FilterResult {
	if filter == nil || filter.attribute == "" || filter.prefix == "" {
		return NoFilter
	}
	value, ok := LookupAttribute(event, filter.attribute)
	if !ok {
		return FailFilter
	}

	if strings.HasPrefix(fmt.Sprintf("%v", value), filter.prefix) {
		return PassFilter
	}
	return FailFilter
}

var _ Filter = &prefixFilter{}
