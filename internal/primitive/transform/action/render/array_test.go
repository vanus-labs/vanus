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

package render_test

import (
	stdJson "encoding/json"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/render"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRenderArrayAction(t *testing.T) {
	funcName := render.NewRenderArrayAction().Name()
	Convey("test render array invalid", t, func() {
		jsonStr := `{
			  "array": [
				{
				  "name": "name1",
				  "number": 1
				},
				{
				  "name": "name2",
				  "number": "2"
				},
				{
				  "name": "name3"
				}
			  ]
		}`
		Convey("render param not same", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.render", "$.data.array", "begin <@.name> <@.number> end"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			var data map[string]interface{}
			err = stdJson.Unmarshal([]byte(jsonStr), &data)
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
				Data:  data,
			})
			So(err, ShouldNotBeNil)
		})
	})
	Convey("test render array valid", t, func() {
		jsonStr := `{
			  "array": [
				{
				  "name": "name1",
				  "number": 1
				},
				{
				  "name": "name2",
				  "number": "2"
				},
				{
				  "name": "name3",
				  "number": "3"
				}
			  ]
		}`
		Convey("render constants", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.render", "$.data.array", "abc"})
			So(err, ShouldBeNil)

			e := cetest.MinEvent()
			var data map[string]interface{}
			err = stdJson.Unmarshal([]byte(jsonStr), &data)
			So(err, ShouldBeNil)
			err = a.Execute(&context.EventContext{
				Event: &e,
				Data:  data,
			})
			So(err, ShouldBeNil)
			render, exist := data["render"]
			So(exist, ShouldBeTrue)
			So(len(render.([]string)), ShouldEqual, 1)
			So(render.([]string)[0], ShouldEqual, "abc")
		})
		Convey("render variable template", func() {
			Convey("render all variable", func() {
				a, err := runtime.NewAction([]interface{}{funcName, "$.data.render", "$.data.array", "<@.name><@.number>"})
				So(err, ShouldBeNil)
				e := cetest.MinEvent()
				var data map[string]interface{}
				err = stdJson.Unmarshal([]byte(jsonStr), &data)
				So(err, ShouldBeNil)
				err = a.Execute(&context.EventContext{
					Event: &e,
					Data:  data,
				})
				So(err, ShouldBeNil)
				render, exist := data["render"]
				So(exist, ShouldBeTrue)
				So(len(render.([]string)), ShouldEqual, 3)
				So(render.([]string)[0], ShouldEqual, "name11")
				So(render.([]string)[1], ShouldEqual, "name22")
				So(render.([]string)[2], ShouldEqual, "name33")
			})
			Convey("render begin and end", func() {
				a, err := runtime.NewAction([]interface{}{funcName, "$.data.render", "$.data.array", "<@.name> and <@.number>"})
				So(err, ShouldBeNil)
				e := cetest.MinEvent()
				var data map[string]interface{}
				err = stdJson.Unmarshal([]byte(jsonStr), &data)
				So(err, ShouldBeNil)
				err = a.Execute(&context.EventContext{
					Event: &e,
					Data:  data,
				})
				So(err, ShouldBeNil)
				render, exist := data["render"]
				So(exist, ShouldBeTrue)
				So(len(render.([]string)), ShouldEqual, 3)
				So(render.([]string)[0], ShouldEqual, "name1 and 1")
				So(render.([]string)[1], ShouldEqual, "name2 and 2")
				So(render.([]string)[2], ShouldEqual, "name3 and 3")
			})
			Convey("render other", func() {
				a, err := runtime.NewAction([]interface{}{funcName, "$.data.render", "$.data.array", "Name: <@.name> Num: <@.number> <@abc"})
				So(err, ShouldBeNil)
				e := cetest.MinEvent()
				var data map[string]interface{}
				err = stdJson.Unmarshal([]byte(jsonStr), &data)
				So(err, ShouldBeNil)
				err = a.Execute(&context.EventContext{
					Event: &e,
					Data:  data,
				})
				So(err, ShouldBeNil)
				render, exist := data["render"]
				So(exist, ShouldBeTrue)
				So(len(render.([]string)), ShouldEqual, 3)
				So(render.([]string)[0], ShouldEqual, "Name: name1 Num: 1 <@abc")
				So(render.([]string)[1], ShouldEqual, "Name: name2 Num: 2 <@abc")
				So(render.([]string)[2], ShouldEqual, "Name: name3 Num: 3 <@abc")
			})
		})
	})
}
