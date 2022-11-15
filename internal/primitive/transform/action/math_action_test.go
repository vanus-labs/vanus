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

package action

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/primitive/transform/context"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMathAction(t *testing.T) {
	Convey("test math add", t, func() {
		a, err := NewAction([]interface{}{newMathAddActionAction().Name(), "$.test", "123", "456", "321"})
		So(err, ShouldBeNil)
		e := newEvent()
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, int32(900))
	})
	Convey("test math sub", t, func() {
		a, err := NewAction([]interface{}{newMathSubActionAction().Name(), "$.test", "456", "123"})
		So(err, ShouldBeNil)
		e := newEvent()
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, int32(333))
	})
	Convey("test math mul", t, func() {
		a, err := NewAction([]interface{}{newMathMulActionAction().Name(), "$.test", "111", "2", "3"})
		So(err, ShouldBeNil)
		e := newEvent()
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, int32(666))
	})
	Convey("test math div", t, func() {
		Convey("div zero", func() {
			a, err := NewAction([]interface{}{newMathDivActionAction().Name(), "$.test", "333", "0"})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("div", func() {
			a, err := NewAction([]interface{}{newMathDivActionAction().Name(), "$.test", "333", "3"})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, int32(111))
		})
	})
}
