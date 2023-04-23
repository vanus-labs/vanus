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
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/datetime"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestConvertTimezoneAction(t *testing.T) {
	funcName := datetime.NewConvertTimezoneAction().Name()
	Convey("test convert timezone", t, func() {
		Convey("test with default time zone", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.time", "CET", "UTC", ""})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{"time": "2021-08-29 12:01:10"},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2021-08-29 10:01:10")
		})
		Convey("test with ist time zone", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.time", "CET", "Asia/Kolkata", ""})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{"time": "2021-08-29 12:01:10"},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2021-08-29 15:31:10")
		})
		Convey("test with date param", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.time", "CET", "Asia/Kolkata", "2006-01-02 15:04:05"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{"time": "2021-08-29 12:01:10"},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2021-08-29 15:31:10")
		})
	})
}
