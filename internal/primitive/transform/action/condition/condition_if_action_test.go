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

package condition_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/condition"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestConditionIfAction(t *testing.T) {
	funcName := condition.NewConditionIfAction().Name()
	Convey("test condition if ==", t, func() {
		Convey("test string", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", "==", "test", true, false})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "test")
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test2"], ShouldEqual, true)
		})
		Convey("test number", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", "==", 123, true, false})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", 123)
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(e.Extensions()["test2"], ShouldEqual, true)
		})
	})
	Convey("test condition >=", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", ">=", int32(123), true, false})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", "456")
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test2"], ShouldEqual, true)
	})
	Convey("test condition >", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", ">", int32(123), true, false})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", 456)
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test2"], ShouldEqual, true)
	})
	Convey("test condition <=", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", "<=", "123", true, false})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", 456)
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test2"], ShouldEqual, false)
	})
	Convey("test condition <", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", "<", "123", true, false})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", 456)
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test2"], ShouldEqual, false)
	})
	Convey("test condition unDefine op invalid", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test2", "$.test", "<==", "123", true, false})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("test", 456)
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldNotBeNil)
	})
}
