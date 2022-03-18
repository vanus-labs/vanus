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
	"github.com/linkall-labs/vanus/observability/log"
	"strings"
)

type suffixFilter struct {
	suffix map[string]string
}

func NewSuffixFilter(suffix map[string]string) Filter {
	for attr, v := range suffix {
		if attr == "" || v == "" {
			log.Info(nil, "new suffix filter but has empty ", map[string]interface{}{
				"attr":  attr,
				"value": v,
			})
			return nil
		}
	}
	return &suffixFilter{suffix: suffix}
}

func (filter *suffixFilter) Filter(event ce.Event) FilterResult {
	if filter == nil {
		return FailFilter
	}
	log.Debug(nil, "suffix filter ", map[string]interface{}{"filter": filter, "event": event})
	for attr, suffix := range filter.suffix {
		value, ok := LookupAttribute(event, attr)
		if !ok {
			return FailFilter
		}
		if !strings.HasSuffix(fmt.Sprintf("%v", value), suffix) {
			return FailFilter
		}
	}
	return PassFilter
}

func (filter *suffixFilter) String() string {
	return fmt.Sprintf("suffix:%v", filter.suffix)
}

var _ Filter = (*suffixFilter)(nil)
