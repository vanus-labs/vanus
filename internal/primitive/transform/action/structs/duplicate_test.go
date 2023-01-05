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
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/structs"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDuplicateAction(t *testing.T) {
	funcName := structs.NewDuplicateAction().Name()
	Convey("test duplicate", t, func() {
		Convey("duplicate target key exist", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "$.data.abc.test"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "abc")
			err = a.Execute(&context.EventContext{
				Event: &e,
				Data: map[string]interface{}{
					"abc": map[string]interface{}{
						"test": "value",
					},
				},
			})
			So(err, ShouldNotBeNil)
		})
		Convey("duplicate", func() {
			a, err := runtime.NewAction([]interface{}{funcName, "$.test", "$.data.abc.test"})
			So(err, ShouldBeNil)
			e := cetest.MinEvent()
			e.SetExtension("test", "abc")
			ceCtx := &context.EventContext{
				Event: &e,
				Data:  map[string]interface{}{},
			}
			err = a.Execute(ceCtx)
			So(err, ShouldBeNil)
			So(e.Extensions()["test"], ShouldEqual, "abc")
			So(ceCtx.Data.(map[string]interface{})["abc"].(map[string]interface{})["test"], ShouldEqual, "abc")
		})
	})
}
