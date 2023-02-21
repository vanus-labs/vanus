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
	"encoding/json"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCheckCustomValuesAction(t *testing.T) {
	funcName := strings.NewCheckCustomValuesAction().Name()
	jsonStr := `{
		"source": "value 2"
}`
	Convey("contains", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.source", "value", "$.data.target", "true", "false"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		var data map[string]interface{}
		err = json.Unmarshal([]byte(jsonStr), &data)
		So(err, ShouldBeNil)
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldEqual, "true")
	})
	Convey("not contains", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.source", "Value", "$.data.target", "true", "false"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		var data map[string]interface{}
		err = json.Unmarshal([]byte(jsonStr), &data)
		So(err, ShouldBeNil)
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldEqual, "false")
	})
	Convey("contains, replacement int", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.source", "value", "$.data.target", 1, 0})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		var data map[string]interface{}
		err = json.Unmarshal([]byte(jsonStr), &data)
		So(err, ShouldBeNil)
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldEqual, 1)
	})
	Convey("source don't exist, runArgs error", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.source2", "value", "$.data.target", "true", "false"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		var data map[string]interface{}
		err = json.Unmarshal([]byte(jsonStr), &data)
		So(err, ShouldBeNil)
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldNotBeNil)
	})
}
