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

package filter_test

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/trigger/filter"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAllFilter(t *testing.T) {
	event := ce.NewEvent()
	event.SetID("testID")
	event.SetSource("testSource")
	f1 := filter.NewExactFilter(map[string]string{
		"id": "testID",
	})
	f2 := filter.NewPrefixFilter(map[string]string{
		"source": "test",
	})
	f3 := filter.NewPrefixFilter(map[string]string{
		"source": "un",
	})

	Convey("all filter pass", t, func() {
		f := filter.NewAllFilter()
		So(f, ShouldBeNil)
		f = filter.NewAllFilter(f1, f2)
		So(f, ShouldNotBeNil)
		result := f.Filter(event)
		So(result, ShouldEqual, filter.PassFilter)
	})
	Convey("all filter fail", t, func() {
		f := filter.NewAllFilter(f1, f3)
		result := f.Filter(event)
		So(result, ShouldEqual, filter.FailFilter)
	})
}
