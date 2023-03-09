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

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestReplaceBetweenPositionsAction(t *testing.T) {
	funcName := strings.NewReplaceBetweenPositionsAction().Name()

	Convey("test Positive testcase", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 7, 12, "Vanus"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "Hello, World!")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "Hello, Vanus!")
	})

	Convey("test Negative testcase: startPosition greater than string length", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 100, 8, "Dan"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "Start position must be less than the length of the string")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
		So(e.Extensions()["test"], ShouldEqual, "Start position must be less than the length of the string")
	})

	Convey("test Negative testcase: endPosition greater than string length", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 8, 60, "free to use"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "End position must be less than the length of the string")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
		So(e.Extensions()["test"], ShouldEqual, "End position must be less than the length of the string")
	})

	Convey("test Negative testcase: startPosition greater than endPosition", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", 12, 5, "Python"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "Start position must be less than end position")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
		So(e.Extensions()["test"], ShouldEqual, "Start position must be less than end position")
	})
}
