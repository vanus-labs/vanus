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
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSplitFromStartAction(t *testing.T) {
	funcName := strings.NewSplitFromStartAction().Name()

	Convey("test split from start: Positive testcase", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 5, "$.data.target"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{}
		e.SetExtension("test", "Hello, World!")
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, []string{"Hello", ", World!"})
	})

	Convey("test split from start: Positive testcase, length 1", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 0, "$.data.target"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{}
		e.SetExtension("test", "H")
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, []string{"H"})
	})

	Convey("test split from start: Negative testcase, split position less than one", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 0, "$.data.target"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{}
		e.SetExtension("test", "split position must be more than zero")
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
		So(e.Extensions()["test"], ShouldEqual, "split position must be more than zero")
	})

	Convey("test split from start: Negative testcase, split position greater than string length", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 100, "$.data.target"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{}
		e.SetExtension("test", "split position must be less than the length of the string")
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
		So(e.Extensions()["test"], ShouldEqual, "split position must be less than the length of the string")
	})
}
