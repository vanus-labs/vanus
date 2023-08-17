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
	"time"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/datetime"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestToday(t *testing.T) {
	funcName := datetime.NewTodayAction().Name()
	Convey("test today", t, func() {
		Convey("test default time zone", func() {
			e := cetest.MinEvent()
			// e.SetExtension("test", "2022-11-15T15:41:25Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, time.Now().In(time.UTC).Format(time.DateOnly))
		})
		Convey("test  time zone", func() {
			e := cetest.MinEvent()
			// e.SetExtension("test", "2022-11-15T15:41:25Z")
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "Europe/Berlin"})
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			loc, _ := time.LoadLocation("Europe/Berlin")
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, time.Now().In(loc).Format(time.DateOnly))
		})
	})
}
