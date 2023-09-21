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

func TestSplitWithIntervalsAction(t *testing.T) {
	funcName := strings.NewSplitWithIntervalsAction().Name()

	Convey("test Positive testcase", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", 2, 2, "$.data.appinfoB"})
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
		So(res, ShouldResemble, []string{"he", "ll", "o ", "wo", "rl", "d!"})
	})

	Convey("test when start position is greater than the length of the string", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", 100, 2, "$.data.appinfoB"})
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
		So(res, ShouldResemble, []string{"hello world!"})
	})

	Convey("test when the string can't be split exactly into equal intervals", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", 1, 3, "$.data.appinfoB"})
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
		So(res, ShouldResemble, []string{"h", "ell", "o w", "orl", "d!"})
	})

	Convey("test when target key exists", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.appinfoA", 2, 2, "$.data.appinfoB"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{
			"appinfoA": "hello world!",
			"appinfoB": "",
		}
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "key $.data.appinfoB exists")
	})
}
