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

package template

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestExecute(t *testing.T) {
	Convey("test string var", t, func() {
		p := NewTemplate()
		p.Parse(`{"key":"<str>"}`)
		Convey("no data", func() {
			m := make(map[string]interface{})
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"<str>"}`)
		})
		Convey("null", func() {
			m := make(map[string]interface{})
			m["str"] = nil
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":""}`)
		})
		Convey("string", func() {
			m := make(map[string]interface{})
			m["str"] = "str"
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"str"}`)
		})
		Convey("number", func() {
			m := make(map[string]interface{})
			m["str"] = 123.456
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"123.456"}`)
		})
		Convey("bool", func() {
			m := make(map[string]interface{})
			m["str"] = true
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"true"}`)
		})
		Convey("object", func() {
			m := make(map[string]interface{})
			m["str"] = map[string]interface{}{"str": true}
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"{}"}`)
		})
		Convey("array", func() {
			m := make(map[string]interface{})
			m["str"] = []interface{}{"str", true}
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"[]"}`)
		})
	})
	Convey("test var", t, func() {
		p := NewTemplate()
		p.Parse(`{"key":<str>}`)
		Convey("no data", func() {
			m := make(map[string]interface{})
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"<str>"}`)
		})
		Convey("null", func() {
			m := make(map[string]interface{})
			m["str"] = nil
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":null}`)
		})
		Convey("string", func() {
			m := make(map[string]interface{})
			m["str"] = "str"
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":"str"}`)
		})
		Convey("number", func() {
			m := make(map[string]interface{})
			m["str"] = 123.456
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":123.456}`)
		})
		Convey("bool", func() {
			m := make(map[string]interface{})
			m["str"] = true
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":true}`)
		})
		Convey("obj", func() {
			m := make(map[string]interface{})
			m["str"] = map[string]interface{}{"k": "v"}
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":{"k":"v"}}`)
		})
		Convey("array", func() {
			m := make(map[string]interface{})
			m["str"] = []interface{}{"str", 123.456}
			v := p.Execute(m)
			So(string(v), ShouldEqual, `{"key":["str",123.456]}`)
		})
	})
}
