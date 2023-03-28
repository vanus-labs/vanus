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
	"fmt"
	"reflect"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"

	"github.com/vanus-labs/vanus/internal/trigger/util"
	"github.com/vanus-labs/vanus/observability/log"
	putil "github.com/vanus-labs/vanus/pkg/util"
)

type commonFilter struct {
	attribute     map[string]string
	dataValue     string
	data          map[string]string
	meetCondition meetCondition
}

type meetCondition func(value, compareValue string) bool

func newCommonFilter(value map[string]string, meetCondition meetCondition) *commonFilter {
	attribute := map[string]string{}
	data := map[string]string{}
	var dataValue string
	for attr, v := range value {
		if attr == "" || v == "" {
			log.Info().Str("attr", attr).
				Str("value", v).
				Msg("new filter but has empty ")
			return nil
		}
		switch {
		case attr == "data":
			dataValue = v
		case len(attr) > 4 && attr[:5] == "data.":
			attr = attr[5:]
			attr = "$." + attr
			data[attr] = v
		default:
			// event attribute.
			attribute[attr] = v
		}
	}
	return &commonFilter{
		attribute:     attribute,
		data:          data,
		dataValue:     dataValue,
		meetCondition: meetCondition,
	}
}

func (filter *commonFilter) Filter(event ce.Event) Result {
	for attr, v := range filter.attribute {
		value, ok := util.LookupAttribute(event, attr)
		if !ok {
			return false
		}
		strValue, err := attrValue2String(value)
		if err != nil {
			log.Info().Str("attr", attr).Err(err).Msg("filter attr value to string failed")
			return false
		}
		if !filter.meetCondition(strValue, v) {
			return FailFilter
		}
	}
	if filter.dataValue != "" && !filter.meetCondition(string(event.Data()), filter.dataValue) {
		return FailFilter
	}
	if len(filter.data) == 0 {
		return PassFilter
	}
	obj, err := putil.ParseJSON(event.Data())
	if err != nil {
		log.Info().Str("data", string(event.Data())).Err(err).Msg("filter parse data error")
		return false
	}
	for attr, v := range filter.data {
		value, err := putil.GetJSONValue(obj, attr)
		if err != nil {
			log.Info().Str("path", attr).Err(err).Msg("filter parse json path error")
			return false
		}
		strValue, err := dataValue2String(value)
		if err != nil {
			log.Info().Str("path", attr).Err(err).Msg("filter data value to string failed")
			return false
		}
		if !filter.meetCondition(strValue, v) {
			return FailFilter
		}
	}
	return PassFilter
}

func dataValue2String(value interface{}) (string, error) {
	v, ok := value.(string)
	if ok {
		return v, nil
	}
	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Bool:
		return fmt.Sprintf("%v", value), nil
	default:
		return "", fmt.Errorf("filter value %s can't convert to string", reflectValue.Kind())
	}
}

func attrValue2String(value interface{}) (string, error) {
	v, ok := value.(string)
	if ok {
		return v, nil
	}
	val, err := types.Format(value)
	return val, err
}
