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
	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/oliveagle/jsonpath"
)

// LookupAttribute lookup event attribute value by attribute name
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

// LookupData lookup event data value by JSON path
func LookupData(data interface{}, path string) (interface{}, error) {
	return jsonpath.JsonPathLookup(data, path)
}
