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

package transformation_test

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/trigger/transformation"
	. "github.com/smartystreets/goconvey/convey"
)

//func TestParseDataVariable(t *testing.T) {
//	keys := "key"
//	Convey("test parse data nil", t, func() {
//		d := transformation.ParseDataVariable(nil, keys)
//		So(d.DataType, ShouldEqual, template.NoExist)
//	})
//
//	Convey("test parse data no exist", t, func() {
//		rs := make(map[string]vjson.Result)
//		rs["key2"] = vjson.Result{Key: "key"}
//		d := transformation.ParseDataVariable(rs, keys)
//		So(d.DataType, ShouldEqual, template.NoExist)
//	})
//
//	Convey("test parse data value nil", t, func() {
//		rs := make(map[string]vjson.Result)
//		rs["key"] = vjson.Result{Key: "key", Type: vjson.Null}
//		d := transformation.ParseDataVariable(rs, keys)
//		So(d.DataType, ShouldEqual, template.Null)
//	})
//
//	Convey("test parse data value string", t, func() {
//		rs := make(map[string]vjson.Result)
//		rs["key"] = vjson.Result{Key: "key", Type: vjson.String}
//		d := transformation.ParseDataVariable(rs, keys)
//		So(d.DataType, ShouldEqual, template.Text)
//	})
//
//	Convey("test parse data value other", t, func() {
//		rs := make(map[string]vjson.Result)
//		rs["key"] = vjson.Result{Key: "key", Type: vjson.Array}
//		d := transformation.ParseDataVariable(rs, keys)
//		So(d.DataType, ShouldEqual, template.Other)
//	})
//
//	Convey("test parse data value many nest", t, func() {
//		rs := make(map[string]vjson.Result)
//		rs["key1"] = vjson.Result{Key: "key1", Type: vjson.Object, Result: map[string]vjson.Result{
//			"key2": {Key: "key2", Type: vjson.String},
//		}}
//		keys = []string{"key1", "key2"}
//		d := transformation.ParseDataVariable(rs, keys)
//		So(d.DataType, ShouldEqual, template.Text)
//	})
//}

func TestParseData(t *testing.T) {
	e := ce.NewEvent()
	e.SetType("testType")
	e.SetSource("testSource")
	e.SetID("testId")
	e.SetExtension("vanusKey", "vanusValue")
	_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
		"key":  "value",
		"key1": "value1",
	})
	input := &primitive.InputTransformer{
		Define: map[string]string{
			"keyTest": "keyValue",
			"ctxId":   "$.id",
			"ctxKey":  "$.vanuskey",
			"data":    "$.data",
			"dataKey": "$.data.key",
		},
		Template: "test ${keyTest} Id ${ctxId} type ${ctxType} data ${data} key ${dateKey}",
	}

	Convey("test parse data", t, func() {
		it := transformation.NewInputTransformer(input)
		m, err := it.ParseData(&e)
		So(err, ShouldBeNil)
		So(m["keyTest"].String(), ShouldEqual, "keyValue")
		So(m["ctxId"].String(), ShouldEqual, e.ID())
		So(m["ctxKey"].String(), ShouldEqual, "vanusValue")
		So(m["dataKey"].String(), ShouldEqual, "value")
	})
}

func TestExecute(t *testing.T) {
	Convey("test execute", t, func() {
		e := ce.NewEvent()
		e.SetType("testType")
		e.SetSource("testSource")
		e.SetID("testId")
		e.SetExtension("vanusKey", "vanusValue")
		input := &primitive.InputTransformer{
			Define: map[string]string{
				"keyTest": "keyValue",
				"ctxId":   "$.id",
				"ctxKey":  "$.vanuskey",
				"data":    "$.data",
				"dataKey": "$.data.key",
			},
		}
		Convey("test execute text", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = "${keyTest} ${ctxId} ${ctxType} ${data} ${dataKey}"
			it := transformation.NewInputTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `keyValue testId  {"key":"value","key1":"value1"} value`)
		})
		Convey("test execute json signal value", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"body": {"data": ${dataKey},"data2": "${dataKey}","data3": ${noExist},"data4": "${noExist}"}}`
			it := transformation.NewInputTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"body": {"data": "value","data2": "value","data3": null,"data4": ""}}`)
		})
		Convey("test execute json with a part of value", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = ` {"body": {"data": "source is ${dataKey}","data2": "source is ${noExist}"}}`
			it := transformation.NewInputTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, ` {"body": {"data": "source is value","data2": "source is "}}`)
		})
		Convey("test execute json with a part of value has colon", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"body": {"data": ":${dataKey}","data2": "\":${dataKey}\"","data3": "::${dataKey}"}}`
			it := transformation.NewInputTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"body": {"data": ":value","data2": "\":value\"","data3": "::value"}}`)
		})
		Convey("test execute json with a part of value has quota", func() {
			_ = e.SetData(ce.ApplicationJSON, map[string]interface{}{
				"key":  "value",
				"key1": "value1",
			})
			input.Template = `{"body": {"data": "source is \"${dataKey}\"","data2": "source is \"${noExist}\""}}`
			it := transformation.NewInputTransformer(input)
			it.Execute(&e)
			So(string(e.Data()), ShouldEqual, `{"body": {"data": "source is \"value\"","data2": "source is \"\""}}`)
		})
	})

}
