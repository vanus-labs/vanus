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

type suffixFilter struct {
	attribute, suffix string
}

// NewSuffixFilter returns an event filter which passes if the value of the context
// attribute in the CloudEvent ends with suffix.
func NewSuffixFilter(attribute, suffix string) (Filter, error) {
	if attribute == "" || suffix == "" {
		return nil, fmt.Errorf("invalid arguments, attribute and suffix can't be empty")
	}
	return &suffixFilter{
		attribute: attribute,
		suffix:    suffix,
	}, nil
}

func (filter *suffixFilter) Filter(ctx context.Context, event cloudevents.Event) FilterResult {
	if filter == nil || filter.attribute == "" || filter.suffix == "" {
		return NoFilter
	}
	value, ok := LookupAttribute(event, filter.attribute)
	if !ok {
		return FailFilter
	}

	if strings.HasSuffix(fmt.Sprintf("%v", value), filter.suffix) {
		return PassFilter
	}
	return FailFilter
}

var _ Filter = &suffixFilter{}
