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

func TestStringAction(t *testing.T) {
	Convey("test join action", t, func() {
		a, err := NewAction([]interface{}{newJoinAction().Name(), "$.test", ",", "abc", "123"})
		So(err, ShouldBeNil)
		e := newEvent()
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "abc,123")
	})
	Convey("test upper", t, func() {
		a, err := NewAction([]interface{}{newUpperAction().Name(), "$.test"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "testValue")
		ceCtx := &context.EventContext{
			Event: e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "TESTVALUE")
	})
	Convey("test lower", t, func() {
		a, err := NewAction([]interface{}{newLowerAction().Name(), "$.test"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "testValue")
		ceCtx := &context.EventContext{
			Event: e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "testvalue")
	})
	Convey("test add prefix", t, func() {
		a, err := NewAction([]interface{}{newAddPrefixAction().Name(), "$.test", "prefix"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "testValue")
		ceCtx := &context.EventContext{
			Event: e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "prefixtestValue")
	})
	Convey("test add suffix", t, func() {
		a, err := NewAction([]interface{}{newAddSuffixAction().Name(), "$.test", "suffix"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "testValue")
		ceCtx := &context.EventContext{
			Event: e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "testValuesuffix")
	})
}
