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

	cetest "github.com/cloudevents/sdk-go/v2/test"

	vContext "github.com/linkall-labs/vanus/internal/primitive/transform/context"
	. "github.com/smartystreets/goconvey/convey"
)

func TestExecute(t *testing.T) {
	Convey("test define", t, func() {
		m := make(map[string]interface{})
		ceCtx := &vContext.EventContext{
			Define: m,
		}
		tp := NewTemplate()
		tp.Parse(`{"key":<str>,"key2":"<str2>"}`)
		Convey("no exist", func() {
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":"<str>","key2":"<str2>"}`)
		})
		newTestExecFunc(tp, ceCtx, m)()
	})
	Convey("test event attribute", t, func() {
		event := cetest.FullEvent()
		m := event.Context.AsV1().Extensions
		ceCtx := &vContext.EventContext{
			Event: &event,
		}
		tp := NewTemplate()
		tp.Parse(`{"key":<$.str>,"key2":"<$.str2>"}`)
		Convey("no exist", func() {
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":"<$.str>","key2":"<$.str2>"}`)
		})
		newTestExecFunc(tp, ceCtx, m)()
	})
	Convey("test event data", t, func() {
		m := make(map[string]interface{})
		ceCtx := &vContext.EventContext{
			Data: m,
		}
		tp := NewTemplate()
		tp.Parse(`{"key":<$.data.str>,"key2":"<$.data.str2>"}`)
		Convey("no exist", func() {
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":"<$.data.str>","key2":"<$.data.str2>"}`)
		})
		newTestExecFunc(tp, ceCtx, m)()
	})
}

func newTestExecFunc(tp *Template, ceCtx *vContext.EventContext, m map[string]interface{}) func() {
	return func() {
		Convey("nil value", func() {
			m["str"] = nil
			m["str2"] = nil
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":null,"key2":""}`)
		})
		Convey("string value", func() {
			m["str"] = "str"
			m["str2"] = "str2"
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":"str","key2":"str2"}`)
		})
		Convey("number value", func() {
			m["str"] = 123.456
			m["str2"] = 321.654
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":123.456,"key2":"321.654"}`)
		})
		Convey("bool value", func() {
			m["str"] = true
			m["str2"] = true
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":true,"key2":"true"}`)
		})
		Convey("object value", func() {
			m["str"] = map[string]interface{}{"str": true}
			m["str2"] = map[string]interface{}{"str2": true}
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":{"str":true},"key2":"{}"}`)
		})
		Convey("array value", func() {
			m["str"] = []interface{}{"str", true}
			m["str2"] = []interface{}{"str2", true}
			v := tp.Execute(ceCtx)
			So(string(v), ShouldEqual, `{"key":["str",true],"key2":"[]"}`)
		})
	}
}
