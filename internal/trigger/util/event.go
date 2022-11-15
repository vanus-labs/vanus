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

package util

import (
	"fmt"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/oliveagle/jsonpath"
)

// LookupAttribute lookup event attribute value by attribute name.
func LookupAttribute(event ce.Event, attr string) (interface{}, bool) {
	switch attr {
	case "specversion":
		return event.SpecVersion(), true
	case "type":
		return event.Type(), true
	case "source":
		return event.Source(), true
	case "subject":
		return event.Subject(), true
	case "id":
		return event.ID(), true
	case "time":
		return event.Time().String(), true
	case "dataschema":
		return event.DataSchema(), true
	case "datacontenttype":
		return event.DataContentType(), true
	default:
		extensions := event.Context.AsV1().Extensions
		if len(extensions) == 0 {
			return nil, false
		}
		val, ok := extensions[attr]
		return val, ok
	}
}

// LookupData lookup event data value by JSON path.
func LookupData(data interface{}, path string) (interface{}, error) {
	v, err := jsonpath.JsonPathLookup(data, path)
	if err != nil {
		if strings.Contains(err.Error(), "not found in object") {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return v, nil
}

var (
	ErrKeyNotFound          = fmt.Errorf("data key not found")
	errValueIsNil           = fmt.Errorf("value is nil")
	errAttributeValue       = fmt.Errorf("attribute value is invalid")
	errDeleteAttrNotSupport = fmt.Errorf("delete attribute not support")

	specAttributes = map[string]struct{}{
		"id":          {},
		"source":      {},
		"type":        {},
		"specversion": {},
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
		switch val := value.(type) {
		case string:
			ts, err := time.Parse(time.RFC3339Nano, val)
			if err != nil {
				return errAttributeValue
			}
			v = ts
		case ce.Timestamp:
			v = val.Time
		case time.Time:
			v = val
		case *time.Time:
			v = *val
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
	event, _ := e.Context.(*ce.EventContextV1)
	if len(event.Extensions) == 0 {
		return nil
	}
	delete(event.Extensions, attr)
	return nil
}

// SetData set value to data path, now data must is map, not support array.
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
	switch m := data.(type) {
	case map[string]interface{}:
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
	switch m := data.(type) {
	case map[string]interface{}:
		if len(paths) == 1 {
			delete(m, paths[0])
			return
		}
		deleteData(m[paths[0]], paths[1:])
	case []interface{}:
		// todo ,now not support
	}
}
