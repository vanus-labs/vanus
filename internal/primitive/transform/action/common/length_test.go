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

package common_test

import (
	stdJson "encoding/json"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/common"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestLengthAction(t *testing.T) {
	funcName := common.NewLengthAction().Name()
	Convey("test length", t, func() {
		jsonStr := `{
			"array": ["123",4,true],
			"string": "string",
			"map":{
				"key1":"value1",
				"key2":"value2"
			},
			"number": 10,
			"bool": false
		}`
		Convey("test array", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.length", "$.data.array"})
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
			So(data["length"], ShouldEqual, 3)
		})
		Convey("test string", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.length", "$.data.string"})
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
			So(data["length"], ShouldEqual, 6)
		})
		Convey("test map", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.length", "$.data.map"})
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
			So(data["length"], ShouldEqual, 2)
		})
		Convey("test bool", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.length", "$.data.bool"})
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
		Convey("test number", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.length", "$.data.number"})
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
}
