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
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

const (
	eventType   = "dev.vanus.example"
	eventSource = "vanussource"
	eventID     = "2022"
)

func makeEvent() cloudevents.Event {
	e := cloudevents.NewEvent()
	e.SetType(eventType)
	e.SetSource(eventSource)
	e.SetID(eventID)
	return e
}

type passFilter struct{}

func (*passFilter) Filter(event cloudevents.Event) FilterResult {
	return PassFilter
}

type failFilter struct{}

func (*failFilter) Filter(event cloudevents.Event) FilterResult {
	return FailFilter
}

type testStruct struct {
	filter Filter
	expect FilterResult
}

func TestAllFilter_Filter(t *testing.T) {
	tests := map[string]testStruct{
		"Empty": {filter: NewAllFilter(), expect: NoFilter},
		"Pass":  {filter: NewAllFilter(&passFilter{}), expect: PassFilter},
		"Fail":  {filter: NewAllFilter(&failFilter{}), expect: FailFilter},
	}

	for name, tc := range tests {
		convey.Convey(name, t, func() {
			convey.So(tc.expect, convey.ShouldEqual, tc.filter.Filter(makeEvent()))
		})
	}
}
