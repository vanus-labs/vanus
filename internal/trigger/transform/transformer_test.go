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

package transform

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive"
)

func TestExecute(t *testing.T) {
	Convey("test execute", t, func() {
		e := ce.NewEvent()
		e.SetType("testType")
		e.SetSource("testSource")
		e.SetID("testId")
		e.SetExtension("vanusKey", "vanusValue")
		input := &primitive.Transformer{
			Define: map[string]string{
				"keyTest": "keyValue",
				"ctxId":   "$.id",
				"ctxKey":  "$.vanuskey",
				"data":    "$.data",
				"dataKey": "$.data.key",
			},
		}
		Convey("test execute json signal value", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"define": <dataKey>, "data": <$.data.key>,"attribute": <$.id>, "noExist": <noExist>, "noExistStr": "<noExist>"}`
			it := NewTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"define": "value", "data": "value","attribute": "testId", "noExist": "<noExist>", "noExistStr": "<noExist>"}`)
		})
		Convey("test execute json with a part of value", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"data": "source is <dataKey>","data2": "source is <noExist>"}`
			it := NewTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"data": "source is value","data2": "source is <noExist>"}`)
		})
		Convey("test execute json with a part of value has colon", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"data": ":<dataKey>","data2": "\":<dataKey>\"","data3": "::<dataKey> other:<ctxId>"}`
			it := NewTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"data": ":value","data2": "\":value\"","data3": "::value other:testId"}`)
		})
		Convey("test execute json with a part of value has quota", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"data": "source is \"<dataKey>\"","data2": "source is \"<noExist>\""}`
			it := NewTransformer(input)
			it.Execute(&e)
			So(e.DataContentType(), ShouldEqual, ce.ApplicationJSON)
			So(string(e.Data()), ShouldEqual, `{"data": "source is \"value\"","data2": "source is \"<noExist>\""}`)
		})
	})
}
