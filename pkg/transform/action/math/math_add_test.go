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

package math_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/pkg/transform/action/math"
	"github.com/vanus-labs/vanus/pkg/transform/context"
	"github.com/vanus-labs/vanus/pkg/transform/runtime"
)

func TestMathAddAction(t *testing.T) {
	funcName := math.NewMathAddAction().Name()
	Convey("test math add", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", "123", "456", "321"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		err = a.Execute(&context.EventContext{
			Event: &e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, int32(900))
	})
}
