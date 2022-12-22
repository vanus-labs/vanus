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

func TestUnixTimeFormatAction(t *testing.T) {
	funcName := datetime.NewUnixTimeFormatAction().Name()
	Convey("test format unix time", t, func() {
		Convey("test with default time zone", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.time", "Y-m-d H:i:s"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{"time": float64(1668498285)},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2022-11-15 07:44:45")
		})
		Convey("test with time zone", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.time", "Y-m-d H:i:s", "EST"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{"time": float64(1668498285)},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2022-11-15 02:44:45")
		})
	})
}
