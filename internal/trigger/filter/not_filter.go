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
	"fmt"

	ce "github.com/cloudevents/sdk-go/v2"
)

type notFilter struct {
	filter Filter
}

func NewNotFilter(f Filter) Filter {
	if f == nil {
		return nil
	}
	return &notFilter{filter: f}
}

func (filter *notFilter) Filter(event ce.Event) Result {
	switch filter.filter.Filter(event) {
	case FailFilter:
		return PassFilter
	case PassFilter:
		return FailFilter
	}
	return PassFilter
}

func (filter *notFilter) String() string {
	return fmt.Sprintf("%s", filter.filter)
}

var _ Filter = (*notFilter)(nil)
