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

package eventfilter

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Filtering result conditions.
const (
	PassFilter FilterResult = "pass"
	FailFilter FilterResult = "fail"
	NoFilter   FilterResult = "no_filter"
)

// FilterResult has the result of the filtering operation.
type FilterResult string

// And implements filter's logical conjunction.
func (x FilterResult) And(y FilterResult) FilterResult {
	if x == NoFilter {
		return y
	}
	if y == NoFilter {
		return x
	}
	if x == PassFilter && y == PassFilter {
		return PassFilter
	}
	return FailFilter
}

// Or implements filter's logical conjunction.
func (x FilterResult) Or(y FilterResult) FilterResult {
	if x == NoFilter {
		return y
	}
	if y == NoFilter {
		return x
	}
	if x == PassFilter || y == PassFilter {
		return PassFilter
	}
	return FailFilter
}

// Filter is an interface representing an event filter of the trigger filter.
type Filter interface {
	// Filter compute the predicate on the provided event and returns the result of the matching
	Filter(ctx context.Context, event cloudevents.Event) FilterResult
}

func LookupAttribute(event cloudevents.Event, attr string) (interface{}, bool) {
	// Set standard context attributes. The attributes available may not be
	// exactly the same as the attributes defined in the current version of the
	// CloudEvents spec.
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
	case "schemaurl":
		return event.DataSchema(), true
	case "datacontenttype":
		return event.DataContentType(), true
	case "datamediatype":
		return event.DataMediaType(), true
	case "datacontentencoding":
		// TODO: use data_base64 when SDK supports it.
		return event.DeprecatedDataContentEncoding(), true
	default:
		val, ok := event.Extensions()[attr]
		return val, ok
	}
}
