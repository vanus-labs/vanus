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

package strings_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	"github.com/linkall-labs/vanus/internal/primitive/transform/action/strings"
	"github.com/linkall-labs/vanus/internal/primitive/transform/context"
	"github.com/linkall-labs/vanus/internal/primitive/transform/runtime"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReplaceBetweenPositionsAction(t *testing.T) {
	Convey("test ReplaceBetweenPositionsAction", t, func() {
		a := NewReplaceBetweenPositionsAction()
		So(a, ShouldNotBeNil)
		So(a.Name(), ShouldEqual, "REPLACE_BETWEEN_POSITIONS")
		So(a.FixedArgs(), ShouldResemble, []arg.TypeList{arg.EventList})

		e := cetest.MinEvent()
		e.SetExtension("test", "Hello, World!")
		ceCtx := &context.EventContext{
			Event: &e,
		}
		args := []interface{}{"$.test", 7, 11, "Vanus"}
		err := a.Execute(ceCtx, args)
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "Hello, Vanus!")
	})
}
