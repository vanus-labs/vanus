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
	"context"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/trigger/util"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/tidwall/gjson"
)

type commonFilter struct {
	attribute     map[string]string
	data          map[string]string
	meetCondition meetCondition
}

type meetCondition func(exist bool, value interface{}, compareValue string) bool

func newCommonFilter(value map[string]string, meetCondition meetCondition) *commonFilter {
	attribute := map[string]string{}
	data := map[string]string{}
	for attr, v := range value {
		if attr == "" || v == "" {
			log.Info(context.Background(), "new filter but has empty ", map[string]interface{}{
				"attr":  attr,
				"value": v,
			})
			return nil
		}
		if attr == "data" {
			data[""] = v
		} else if len(attr) > 4 && attr[:5] == "data." {
			attr = attr[5:]
			data[attr] = v
		} else {
			// event attribute.
			attribute[attr] = v
		}
	}
	return &commonFilter{
		attribute:     attribute,
		data:          data,
		meetCondition: meetCondition,
	}
}

func (filter *commonFilter) Filter(event ce.Event) Result {
	for attr, v := range filter.attribute {
		value, ok := util.LookupAttribute(event, attr)
		if !filter.meetCondition(ok, value, v) {
			return FailFilter
		}
	}
	for attr, v := range filter.data {
		if attr == "" {
			// event data
			if !filter.meetCondition(true, string(event.Data()), v) {
				return FailFilter
			}
			continue
		}
		result := gjson.GetBytes(event.Data(), attr)
		if !filter.meetCondition(result.Exists(), result.Value(), v) {
			return FailFilter
		}
	}
	return PassFilter
}
