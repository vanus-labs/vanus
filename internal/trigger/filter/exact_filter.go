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
)

type exactFilter struct {
	exact map[string]string
}

func NewExactFilter(exact map[string]string) Filter {
	for attr, v := range exact {
		if attr == "" || v == "" {
			log.Info("new exact filter but has empty ", map[string]interface{}{
				"attr":  attr,
				"value": v,
			})
			return nil
		}
	}
	return &exactFilter{exact: exact}
}

func (filter *exactFilter) Filter(event ce.Event) FilterResult {
	if filter == nil {
		return FailFilter
	}
	log.Debug("exact filter ", map[string]interface{}{"filter": filter, "event": event})
	for attr, v := range filter.exact {
		value, ok := LookupAttribute(event, attr)
		if !ok || value != v {
			return FailFilter
		}
	}
	return PassFilter
}

func (filter *exactFilter) String() string {
	return fmt.Sprintf("exact:%v", filter.exact)
}

var _ Filter = (*exactFilter)(nil)
