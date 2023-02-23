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
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSplitWithIntervalsAction(t *testing.T) {
	funcName := strings.NewSplitWithIntervalsAction().Name()

	Convey("test Positive testcase", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.appinfoA", 2, 2, "$.appinfoB"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("appInfoA", "hello world!")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		expected, err := types.ToBinary(e.Extensions()["appinfob"])
		So(err, ShouldBeNil)
		So(string(expected), ShouldEqual, `["he","ll","o ","wo","rl","d!"]`)
	})

	Convey("test when start position is greater than the length of the string", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.appinfoA", 100, 2, "$.appinfoB"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("appInfoA", "hello world!")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "start position must be less than the length of the string")
	})

	Convey("test when the string can't be split exactly into equal intervals", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.appinfoA", 1, 3, "$.appinfoB"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("appInfoA", "hello world!")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		expected, err := types.ToBinary(e.Extensions()["appinfob"])
		So(err, ShouldBeNil)
		So(string(expected), ShouldEqual, `["h","ell","o w","orl","d!"]`)
	})

	Convey("test when target key exists", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.appinfoA", 2, 2, "$.appinfoB"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		e.SetExtension("appInfoA", "hello world!")
		// Set the target key to already exist
		e.SetExtension("appinfoB", "existing value")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		err = a.Execute(ceCtx)
		So(err.Error(), ShouldEqual, "key $.appinfoB exist")
	})
}
