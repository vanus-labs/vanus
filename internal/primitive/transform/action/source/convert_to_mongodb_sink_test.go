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

package source_test

import (
	stdJson "encoding/json"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/source"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDebeziumToMongoDBSinkAction(t *testing.T) {
	funcName := source.NewDebeziumConvertToMongoDBSink().Name()
	debeziumOp := "iodebeziumop"
	Convey("test convert mongodb sink invalid", t, func() {
		jsonStr := `{
			"num": 123,
			"name": "vanus",
			"desc": "vanus is good"
		}`
		Convey("test insert", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "num", "$.data.num", "name", "$.data.name"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension(debeziumOp, "c")
			var data map[string]interface{}
			err = stdJson.Unmarshal([]byte(jsonStr), &data)
			So(err, ShouldBeNil)
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  data,
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			result := ceCtx.Data.(map[string]interface{})
			So(result["inserts"], ShouldNotBeNil)
			So(len(result["inserts"].([]interface{})), ShouldEqual, 1)
			inserts := result["inserts"].([]interface{})[0].(map[string]interface{})
			So(inserts["num"], ShouldEqual, 123)
			So(inserts["name"], ShouldEqual, "vanus")
			So(inserts["desc"], ShouldEqual, "vanus is good")
		})
		Convey("test update", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "num", "$.data.num", "name", "$.data.name"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension(debeziumOp, "u")
			var data map[string]interface{}
			err = stdJson.Unmarshal([]byte(jsonStr), &data)
			So(err, ShouldBeNil)
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  data,
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			result := ceCtx.Data.(map[string]interface{})
			So(result["updates"], ShouldNotBeNil)
			So(len(result["updates"].([]interface{})), ShouldEqual, 1)
			updates := result["updates"].([]interface{})[0].(map[string]interface{})
			So(updates["filter"].(map[string]interface{})["name"], ShouldEqual, "vanus")
			So(updates["filter"].(map[string]interface{})["num"], ShouldEqual, 123)
			So(updates["update"].(map[string]interface{})["$set"].(map[string]interface{})["desc"],
				ShouldEqual, "vanus is good")
		})
		Convey("test delete", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "num", "$.data.num", "name", "$.data.name"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension(debeziumOp, "d")
			var data map[string]interface{}
			err = stdJson.Unmarshal([]byte(jsonStr), &data)
			So(err, ShouldBeNil)
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  data,
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			result := ceCtx.Data.(map[string]interface{})
			So(result["deletes"], ShouldNotBeNil)
			So(len(result["deletes"].([]interface{})), ShouldEqual, 1)
			deletes := result["deletes"].([]interface{})[0].(map[string]interface{})
			So(deletes["filter"].(map[string]interface{})["name"], ShouldEqual, "vanus")
			So(deletes["filter"].(map[string]interface{})["num"], ShouldEqual, 123)
		})
	})
}
