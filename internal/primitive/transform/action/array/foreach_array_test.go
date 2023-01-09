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

package array_test

import (
	stdJson "encoding/json"
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/array"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReplaceArrayAction(t *testing.T) {
	funcName := array.NewForeachArrayAction().Name()
	Convey("test replace array valid", t, func() {
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
		Convey("replace valid", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.data.array", []interface{}{"add_prefix", "@.name", "prefix"}})
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
			value, exist := data["array"]
			So(exist, ShouldBeTrue)
			So(len(value.([]interface{})), ShouldEqual, 3)
			So(value.([]interface{})[0].(map[string]interface{})["name"], ShouldEqual, "prefixname1")
		})
	})
}
