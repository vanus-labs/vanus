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

package arg

import (
	"testing"

	vContext "github.com/linkall-labs/vanus/internal/primitive/transform/context"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestNewArg(t *testing.T) {
	Convey("test new arg", t, func() {
		Convey("test new event data", func() {
			arg, err := NewArg("$.data.key")
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, EventData)
			So(arg.Name(), ShouldEqual, "key")
		})
		Convey("test new event attribute", func() {
			arg, err := NewArg("$.source")
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, EventAttribute)
			So(arg.Name(), ShouldEqual, "source")
			arg, err = NewArg("$.source_")
			So(err, ShouldNotBeNil)
		})
		Convey("test new define", func() {
			arg, err := NewArg("<var>")
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, Define)
			So(arg.Name(), ShouldEqual, "var")
		})
		Convey("test constants string", func() {
			arg, err := NewArg("data.key")
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, Constant)
			So(arg.Name(), ShouldEqual, "data.key")
		})
		Convey("test constants number", func() {
			arg, err := NewArg(3)
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, Constant)
			So(arg.Name(), ShouldEqual, "3")
		})
		Convey("test constants bool", func() {
			arg, err := NewArg(true)
			So(err, ShouldBeNil)
			So(arg.Type(), ShouldEqual, Constant)
			So(arg.Name(), ShouldEqual, "true")
		})
	})
}

func TestArgEvaluate(t *testing.T) {
	Convey("test new arg evaluate", t, func() {
		event := ce.NewEvent()
		event.SetID("idValue")
		event.SetSource("sourceValue")
		ceCtx := &vContext.EventContext{
			Event: &event,
			Define: map[string]interface{}{
				"var1": "str",
				"var2": 123.456,
				"var3": true,
			},
			Data: map[string]interface{}{
				"key1": "strData",
				"key2": 456.123,
			},
		}
		Convey("test event data", func() {
			arg, err := NewArg("$.data.key1")
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "strData")
			arg, err = NewArg("$.data.key2")
			So(err, ShouldBeNil)
			v, err = arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 456.123)
		})
		Convey("test event attribute", func() {
			arg, err := NewArg("$.source")
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "sourceValue")
			arg, err = NewArg("$.abc")
			So(err, ShouldBeNil)
			v, err = arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldBeNil)
		})
		Convey("test define", func() {
			arg, err := NewArg("<var1>")
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "str")
			arg, err = NewArg("<var2>")
			So(err, ShouldBeNil)
			v, err = arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 123.456)
		})
		Convey("test define empty", func() {
			arg, err := NewArg("<var100>")
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldBeNil)
			ceCtx.Define = map[string]interface{}{}
			arg, err = NewArg("<var2>")
			So(err, ShouldBeNil)
			v, err = arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldBeNil)
		})
		Convey("test constants string", func() {
			arg, err := NewArg("data.key")
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "data.key")
		})
		Convey("test constants number", func() {
			arg, err := NewArg(3)
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, 3)
		})
		Convey("test constants bool", func() {
			arg, err := NewArg(true)
			So(err, ShouldBeNil)
			v, err := arg.Evaluate(ceCtx)
			So(err, ShouldBeNil)
			So(v, ShouldEqual, true)
		})
	})
}
