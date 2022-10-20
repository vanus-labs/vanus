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

package pipeline

import (
	"fmt"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
)

var (
	errValueIsNil           = fmt.Errorf("value is nil")
	errAttributeValue       = fmt.Errorf("attribute value is invalid")
	errDeleteAttrNotSupport = fmt.Errorf("delete attribute not support")

	specAttributes = map[string]struct{}{
		"id":              {},
		"source":          {},
		"type":            {},
		"datacontenttype": {},
		"subject":         {},
		"time":            {},
		"specversion":     {},
		"dataschema":      {},
	}
)

func SetAttribute(e *ce.Event, attr string, value interface{}) error {
	if value == nil {
		return errValueIsNil
	}
	event := e.Context
	switch attr {
	case "type":
		v, ok := value.(string)
		if !ok {
			return errAttributeValue
		}
		return event.SetType(v)
	case "source":
		v, ok := value.(string)
		if !ok {
			return errAttributeValue
		}
		return event.SetSource(v)
	case "subject":
		v, ok := value.(string)
		if !ok {
			return errAttributeValue
		}
		return event.SetSubject(v)
	case "id":
		v, ok := value.(string)
		if !ok {
			return errAttributeValue
		}
		return event.SetID(v)
	case "time":
		var v time.Time
		switch value.(type) {
		case string:
			ts, err := time.Parse(time.RFC3339Nano, value.(string))
			if err != nil {
				return errAttributeValue
			}
			v = ts
		case ce.Timestamp:
			v = value.(ce.Timestamp).Time
		case time.Time:
			v = value.(time.Time)
		case *time.Time:
			v = *value.(*time.Time)
		default:
			return errAttributeValue
		}
		return event.SetTime(v)
	case "dataschema":
		v, ok := value.(string)
		if !ok {
			return errAttributeValue
		}
		return event.SetDataSchema(v)
	case "datacontenttype", "specversion":
		return fmt.Errorf("attribute %s not support modify", attr)
	default:
		return event.SetExtension(attr, value)
	}
}

func DeleteAttribute(e *ce.Event, attr string) error {
	if _, exist := specAttributes[attr]; exist {
		return errDeleteAttrNotSupport
	}
	event := e.Context.(*ce.EventContextV1)
	if len(event.Extensions) == 0 {
		return nil
	}
	delete(event.Extensions, attr)
	return nil
}

// SetData set value to data path, now data must is map, not support array
func SetData(data interface{}, path string, value interface{}) {
	paths := strings.Split(path, ".")
	switch data.(type) {
	case map[string]interface{}:
		setData(data, paths, value)
	case []interface{}:
		// todo ,now not support
	}
}

func setData(data interface{}, paths []string, value interface{}) {
	switch data.(type) {
	case map[string]interface{}:
		m := data.(map[string]interface{})
		if len(paths) == 1 {
			m[paths[0]] = value
			return
		}
		v, ok := m[paths[0]]
		if !ok {
			v = make(map[string]interface{})
			m[paths[0]] = v
		}
		setData(v, paths[1:], value)
	case []interface{}:
		// todo ,now not support
	}
}

func DeleteData(data interface{}, path string) error {
	paths := strings.Split(path, ".")
	switch data.(type) {
	case map[string]interface{}:
		deleteData(data, paths)
	case []interface{}:
		// todo ,now not support
	}
	return nil
}

func deleteData(data interface{}, paths []string) {
	switch data.(type) {
	case map[string]interface{}:
		m := data.(map[string]interface{})
		if len(paths) == 1 {
			delete(m, paths[0])
			return
		}
		deleteData(m[paths[0]], paths[1:])
	case []interface{}:
		// todo ,now not support
	}
}
