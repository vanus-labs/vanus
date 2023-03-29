// Copyright 2023 Linkall Inc.
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
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/tidwall/gjson"
	"github.com/vanus-labs/vanus/observability/log"

	"github.com/vanus-labs/vanus/internal/trigger/util"
)

type commonFilter struct {
	attribute     map[string]string
	data          map[string]string
	meetCondition meetCondition
}

type meetCondition func(value, compareValue string) bool

func newCommonFilter(value map[string]string, meetCondition meetCondition) *commonFilter {
	attribute := map[string]string{}
	data := map[string]string{}
	for attr, v := range value {
		if attr == "" || v == "" {
			log.Info().Str("attr", attr).
				Str("value", v).
				Msg("new filter but has empty ")
			return nil
		}
		switch {
		case attr == "data":
			data[""] = v
		case len(attr) > 4 && attr[:5] == "data.":
			attr = attr[5:]
			data[attr] = v
		default:
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
		if !ok {
			return false
		}
		if !filter.meetCondition(attrValue2String(value), v) {
			return FailFilter
		}
	}
	for attr, v := range filter.data {
		if attr == "" {
			// event data
			if !filter.meetCondition(string(event.Data()), v) {
				return FailFilter
			}
			continue
		}
		result := gjson.GetBytes(event.Data(), attr)
		if !result.Exists() {
			return false
		}
		if !filter.meetCondition(result.String(), v) {
			return FailFilter
		}
	}
	return PassFilter
}

func attrValue2String(value interface{}) string {
	v, ok := value.(string)
	if ok {
		return v
	}
	v, _ = types.Format(value)
	return v
}
