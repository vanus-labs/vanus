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

func TestCapitalizeWordAction(t *testing.T) {
	funcName := strings.NewCapitalizeWordAction().Name()
	Convey("test capitalize word: several words", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "one two three")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "One Two Three")
	})
	Convey("test capitalize word: extra spaces, numbers, other symbols", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", " . one, two,   three q four 111 плюс минус  ")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, " . One, Two,   Three Q Four 111 Плюс Минус  ")
	})
	Convey("test capitalize word: empty string", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "")
	})
	Convey("test capitalize word: unicode ♬", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "♬")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "♬")
	})
	Convey("test capitalize word: a", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "a")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "A")
	})
	Convey("test capitalize word: leading symbols", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "let 'em go")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "Let 'em Go")
	})
}
