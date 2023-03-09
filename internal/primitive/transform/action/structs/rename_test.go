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

package structs_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/structs"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestRenameAction(t *testing.T) {
	funcName := structs.NewRenameAction().Name()
	Convey("test rename", t, func() {
		Convey("rename target key exist", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "$.test2"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "abc")
			e.SetExtension("test2", "abc2")
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldNotBeNil)
		})
		Convey("rename", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "$.test2"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: &e,
			})
			So(err, ShouldBeNil)
			So(len(e.Extensions()), ShouldEqual, 1)
			So(e.Extensions()["test2"], ShouldEqual, "abc")
		})
	})
}
