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

	ce "github.com/cloudevents/sdk-go/v2"
)

func LookupAttribute(event ce.Event, attr string) (string, bool) {
	// Set standard context attributes. The attributes available may not be
	// exactly the same as the attributes defined in the current version of the
	// ce spec.
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
		val, ok := event.Extensions()[attr]
		var str string
		if ok {
			str = fmt.Sprintf("%v", val)
		}
		return str, ok
	}
}
