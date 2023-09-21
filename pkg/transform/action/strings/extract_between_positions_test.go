// Copyright 2023 Linkall Inc.
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

package strings_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/pkg/transform/action/strings"
	"github.com/vanus-labs/vanus/pkg/transform/context"
	"github.com/vanus-labs/vanus/pkg/transform/runtime"
)

func TestExtractBetweenPositionsAction(t *testing.T) {
	funcName := strings.NewExtractBetweenPositionsAction().Name()

	Convey("test: Positive testcase", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 2, 4})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["appinfoB"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, "ell")
	})

	Convey("test: Positive testcase - extract positions on edges", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 1, 12})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["appinfoB"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, "hello world!")
	})

	Convey("test: Positive testcase - extract positions are equal", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 1, 1})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["appinfoB"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, "h")
	})

	Convey("test: Negative testcase - start position more than the length of the string", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 13, 13})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "start position must be equal or less than the length of the string")
	})

	Convey("test: Negative testcase - start position less than one", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 0, 13})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "start position must be more than zero")
	})

	Convey("test: Negative testcase - end position bigger than the length of the string", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 5, 13})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "end position must be equal or less than the length of the string")
	})

	Convey("test: Negative testcase - start position bigger than end position", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", "$.data.appinfoB", 5, 3})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "start position must be be equal or less than end position")
	})
}
