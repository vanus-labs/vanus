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

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewAction(t *testing.T) {
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
			_, err := NewAction([]interface{}{"delete"})
			So(err, ShouldNotBeNil)
		})
		Convey("func arity number greater than", func() {
			_, err := NewAction([]interface{}{"delete", "arg1", "arg2"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new arg error", func() {
			_, err := NewAction([]interface{}{"delete", "$.a-b"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new arg type is invalid", func() {
			_, err := NewAction([]interface{}{"delete", "arg"})
			So(err, ShouldNotBeNil)
		})
		Convey("func new valid", func() {
			_, err := NewAction([]interface{}{"delete", "$.id"})
			So(err, ShouldBeNil)
		})
	})
}
