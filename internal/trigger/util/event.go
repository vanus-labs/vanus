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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
	"github.com/pkg/errors"
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
		return event.Time().Format(time.RFC3339), true
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
	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	p, err := jp.ParseString(path)
	if err != nil {
		return nil, err
	}
	obj, err := oj.Parse(d)
	if err != nil {
		return nil, err
	}
	res := p.Get(obj)
	if len(res) == 0 {
		return nil, ErrKeyNotFound
	} else if len(res) == 1 {
		return res[0], nil
	}
	return res, nil
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
		return errors.Errorf("attribute %s not support modify", attr)
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
func SetData(data interface{}, path string, value interface{}) error {
	paths := strings.Split(path, ".")
	switch data.(type) {
	case map[string]interface{}:
		return setData(data, paths, value)
	case []interface{}:
		// todo ,now not support
	}
	return errors.New("not support")
}

func setData(data interface{}, paths []string, value interface{}) error {
	pathType, key, index, err := getPathIndex(paths[0])
	if err != nil {
		return err
	}
	switch m := data.(type) {
	case map[string]interface{}:
		switch pathType {
		case pathMap:
			// key .
			if len(paths) == 1 {
				m[key] = value
				return nil
			}
			v, ok := m[key]
			if !ok || v == nil {
				v = map[string]interface{}{}
				m[key] = v
			}
			return setData(v, paths[1:], value)
		case pathArray:
			// arr[2] .
			v, ok := m[key]
			if !ok || v == nil {
				m[key] = make([]interface{}, index+1)
			} else {
				vv, ok := v.([]interface{})
				if !ok {
					return errors.Errorf("json path %s is array, but value is not array", paths[0])
				}
				for i := len(vv); i <= index; i++ {
					vv = append(vv, nil)
				}
				m[key] = vv
			}
			return setData(m[key], paths, value)
		}
	case []interface{}:
		if len(paths) == 1 {
			// arr[2].
			m[index] = value
			return nil
		}
		v := m[index]
		if v == nil {
			m[index] = map[string]interface{}{}
		} else {
			_, ok := v.(map[string]interface{})
			if !ok {
				// todo multidimensional array
				return errors.Errorf("json path %s is array, but index %d value is not map", paths[0], index)
			}
		}
		return setData(m[index], paths[1:], value)
	}
	return errors.New("not support")
}

type pathType string

const (
	pathMap   pathType = "map"
	pathArray pathType = "array"
)

func getPathIndex(path string) (pathType, string, int, error) {
	x := strings.Index(path, "[")
	if x <= 0 {
		return pathMap, path, 0, nil
	}
	y := strings.Index(path[x+1:], "]")
	if y <= 0 {
		return pathMap, path, 0, nil
	}
	index := path[x+1 : x+1+y]
	v, err := strconv.ParseInt(index, 10, 32)
	if err != nil {
		// todo map or array
		return pathMap, path, 0, errors.Wrapf(err, "json path %s get array index error, get a not number value", path)
	}
	if v < 0 {
		return pathArray, path[:x], 0, errors.Errorf("json path %s get array index get a negative number", path)
	}
	return pathArray, path[:x], int(v), nil
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
