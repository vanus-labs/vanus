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

package json

import (
	// standard libraries.
	"testing"
	"time"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/pkg/template"
)

func TestTemplate_Execute(t *testing.T) {
	Convey("refer variables", t, func() {
		model := map[string]interface{}{}
		variables := map[string]interface{}{}

		tp, err := Compile(`{"key":<var>,"key2":"<var2>"}`)
		So(err, ShouldBeNil)

		Convey("not exist", func() {
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":null,"key2":""}`)
		})

		newTestExecFunc(tp, model, variables, variables)()
	})

	Convey("refer model", t, func() {
		model := map[string]interface{}{}
		variables := map[string]interface{}{}

		tp, err := Compile(`{"key":<$.var>,"key2":"<$.var2>"}`)
		So(err, ShouldBeNil)

		Convey("not exist", func() {
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key2":""}`)
		})

		newTestExecFunc(tp, model, variables, model)()
	})

	Convey("refer data of model", t, func() {
		data := map[string]interface{}{}
		model := map[string]interface{}{
			"data": data,
		}
		variables := map[string]interface{}{}

		tp, err := Compile(`{"key":<$.data["var"]>,"key2":"<$.data['var2']>"}`)
		So(err, ShouldBeNil)

		Convey("no exist", func() {
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key2":""}`)
		})

		newTestExecFunc(tp, model, variables, data)()
	})
}

func newTestExecFunc(tp template.Template, model any, variables map[string]any, m map[string]any) func() {
	return func() {
		Convey("nil value", func() {
			m["var"] = nil
			m["var2"] = nil
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":null,"key2":"null"}`)
		})

		Convey("string value", func() {
			m["var"] = "var"
			m["var2"] = "var2"
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":"var","key2":"var2"}`)
		})

		Convey("number value", func() {
			m["var"] = 123.456
			m["var2"] = 321.654
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":123.456,"key2":"321.654"}`)
		})

		Convey("bool value", func() {
			m["var"] = true
			m["var2"] = true
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":true,"key2":"true"}`)
		})

		Convey("object value", func() {
			m["var"] = map[string]interface{}{"str": "a\r\nb"}
			m["var2"] = map[string]interface{}{"str2": "b\r\na"}
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":{"str":"a\r\nb"},"key2":"{\"str2\":\"b\\r\\na\"}"}`)
		})

		Convey("array value", func() {
			m["var"] = []interface{}{"str", true, "a\r\nb"}
			m["var2"] = []interface{}{"str2", true, "b\r\na"}
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":["str",true,"a\r\nb"],"key2":"[\"str2\",true,\"b\\r\\na\"]"}`)
		})

		Convey("string with special symbol", func() {
			m["var"] = "<a\r\nb>"
			m["var2"] = "<b\r\na>"
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":"<a\r\nb>","key2":"<b\r\na>"}`)
		})

		Convey("time value", func() {
			tt, _ := time.Parse(time.RFC3339, "2018-04-05T17:31:00Z")
			m["var"] = tt
			m["var2"] = tt
			v, err := tp.Execute(model, variables)
			So(err, ShouldBeNil)
			So(string(v), ShouldEqual, `{"key":"2018-04-05T17:31:00Z","key2":"\"2018-04-05T17:31:00Z\""}`)
		})
	}
}
