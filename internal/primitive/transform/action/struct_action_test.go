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

func TestStructAction(t *testing.T) {
	Convey("test delete", t, func() {
		Convey("delete cant not delete key", func() {
			a, err := NewAction([]interface{}{newDeleteAction().Name(), "$.id"})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("delete", func() {
			a, err := NewAction([]interface{}{newDeleteAction().Name(), "$.test"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldBeNil)
			So(len(e.Extensions()), ShouldEqual, 0)
		})
	})
	Convey("test create", t, func() {
		Convey("create exist key", func() {
			a, err := NewAction([]interface{}{newCreateActionAction().Name(), "$.test", "newValue"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("creat invalid value", func() {
			a, err := NewAction([]interface{}{newCreateActionAction().Name(), "$.test", map[string]interface{}{"a": "b"}})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("creat", func() {
			a, err := NewAction([]interface{}{newCreateActionAction().Name(), "$.test", "testValue"})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "testValue")
		})
	})
	Convey("test replace", t, func() {
		Convey("replace no exist key", func() {
			a, err := NewAction([]interface{}{newReplaceAction().Name(), "$.test", "newValue"})
			So(err, ShouldBeNil)
			e := newEvent()
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("replace", func() {
			a, err := NewAction([]interface{}{newReplaceAction().Name(), "$.test", "testValue"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "testValue")
		})
	})
	Convey("test move", t, func() {
		Convey("move target key exist", func() {
			a, err := NewAction([]interface{}{newMoveActionAction().Name(), "$.test", "$.data.abc.test"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: e,
				Data: map[string]interface{}{
					"abc": map[string]interface{}{
						"test": "value",
					},
				},
			})
			So(err, ShouldNotBeNil)
		})
		Convey("move", func() {
			a, err := NewAction([]interface{}{newMoveActionAction().Name(), "$.test", "$.data.abc.test"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			ceCtx := &context.EventContext{
				Event: e,
				Data:  map[string]interface{}{},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(len(e.Extensions()), ShouldEqual, 0)
			So(ceCtx.Data.(map[string]interface{})["abc"].(map[string]interface{})["test"], ShouldEqual, "abc")
		})
	})
	Convey("test rename", t, func() {
		Convey("rename target key exist", func() {
			a, err := NewAction([]interface{}{newRenameActionAction().Name(), "$.test", "$.test2"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			e.SetExtension("test2", "abc2")
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("rename", func() {
			a, err := NewAction([]interface{}{newRenameActionAction().Name(), "$.test", "$.test2"})
			So(err, ShouldBeNil)
			e := newEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: e,
			})
			So(err, ShouldBeNil)
			So(len(e.Extensions()), ShouldEqual, 1)
			So(e.Extensions()["test2"], ShouldEqual, "abc")
		})
	})
}
