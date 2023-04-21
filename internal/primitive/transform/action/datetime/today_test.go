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

package datetime_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/datetime"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTodayAction(t *testing.T) {
	funcName := datetime.NewTodayAction().Name()
	Convey("test today date", t, func() {
		Convey("test utc time with default time zone", func() {
			e := cetest.MinEvent()
			e.SetExtension("test", "2022-11-15T15:41:25Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "2022-11-15")
		})
		Convey("test with time zone that has same date", func() {
			e := cetest.MinEvent()
			e.SetExtension("test", "2022-11-15T15:41:25Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "EST"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "2022-11-15")
		})
		Convey("test utc time with time zone that has the next date", func() {
			e := cetest.MinEvent()
			e.SetExtension("test", "2022-11-15T23:59:00Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "Asia/Kolkata"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "2022-11-16")
		})
		Convey("test utc time with time zone that has the previous date", func() {
			e := cetest.MinEvent()
			e.SetExtension("test", "2022-11-15T00:59:00Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "America/Chicago"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "2022-11-14")
		})
	})
}
