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

package array_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/internal/primitive/transform/action/array"
	"github.com/vanus-labs/vanus/internal/primitive/transform/context"
	"github.com/vanus-labs/vanus/internal/primitive/transform/runtime"
)

func TestUnfoldArrayAction(t *testing.T) {
	funcName := array.NewUnfoldArrayAction().Name()
	Convey("test unfold array valid", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.array", "$.data.target"})
		So(err, ShouldBeNil)

		e := cetest.MinEvent()
		data := map[string]interface{}{
			"array": []interface{}{"one", 2, []string{"three"}},
		}
		err = a.Execute(&context.EventContext{
			Event: &e,
			Data:  data,
		})
		So(err, ShouldBeNil)
		So(data, ShouldHaveLength, 4)
		So(data["target-0"], ShouldEqual, "one")
		So(data["target-1"], ShouldEqual, 2)
		So(data["target-2"], ShouldResemble, []interface{}{"three"})
	})
	Convey("test unfold array string input", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.data.array", "$.data.target"})
		So(err, ShouldBeNil)

		e := cetest.MinEvent()
		data := map[string]interface{}{
			"array": "one",
		}
		err = a.Execute(&context.EventContext{
			Event: &e,
			Data:  data,
		})
		So(err, ShouldBeNil)
		So(data, ShouldHaveLength, 2)
		So(data["target-0"], ShouldEqual, "one")
	})
}
