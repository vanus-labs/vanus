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

package strings_test

import (
	"testing"

	cetest "github.com/cloudevents/sdk-go/v2/test"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/vanus-labs/vanus/pkg/transform/action/strings"
	"github.com/vanus-labs/vanus/pkg/transform/context"
	"github.com/vanus-labs/vanus/pkg/transform/runtime"
)

func TestSplitWithDelimiterAction(t *testing.T) {
	funcName := strings.NewSplitWithDelimiterAction().Name()
	Convey("test split with delimiter", t, func() {
		a, err := runtime.NewAction([]interface{}{funcName, "$.test", ",", "$.data.target"})
		So(err, ShouldBeNil)
		e := cetest.MinEvent()
		data := map[string]interface{}{}
		e.SetExtension("test", "one,two,three")
		ceCtx := &context.EventContext{
			Event: &e,
			Data:  data,
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		res, ok := data["target"]
		So(ok, ShouldBeTrue)
		So(res, ShouldResemble, []string{"one", "two", "three"})
	})
}
