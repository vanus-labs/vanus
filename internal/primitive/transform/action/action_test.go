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

package action

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/arg"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/function"
	. "github.com/smartystreets/goconvey/convey"
)

func newTestAction() Action {
	return &SourceTargetSameAction{
		FunctionAction{
			CommonAction{
				ActionName: "test_func",
				FixedArgs:  []arg.TypeList{arg.EventList},
				Fn:         function.LowerFunction,
			},
		},
	}
}

func TestNewAction(t *testing.T) {
	AddAction(newTestAction)
	Convey("test new action", t, func() {
		Convey("func name is not string", func() {
			_, err := NewAction([]interface{}{123})
			So(err, ShouldNotBeNil)
		})
		Convey("func name no exist", func() {
			_, err := NewAction([]interface{}{"UnknownCommand"})
			So(err, ShouldNotBeNil)
		})
		Convey("func arity not enough", func() {
			_, err := NewAction([]interface{}{"test_func"})
			So(err, ShouldNotBeNil)
		})
		Convey("func arity number greater than", func() {
			_, err := NewAction([]interface{}{"test_func", "arg1", "arg2"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new arg error", func() {
			_, err := NewAction([]interface{}{"test_func", "$.a-b"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new arg type is invalid", func() {
			_, err := NewAction([]interface{}{"test_func", "arg"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new valid", func() {
			a, err := NewAction([]interface{}{"test_func", "$.id"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
		})
	})
}
