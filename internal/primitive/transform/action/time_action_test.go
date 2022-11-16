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

	"github.com/linkall-labs/vanus/internal/primitive/transform/context"

	ce "github.com/cloudevents/sdk-go/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func newEvent() *ce.Event {
	e := ce.NewEvent()
	e.SetID("testID")
	e.SetType("testType")
	e.SetSource("testSource")
	return &e
}

func TestFormatAction(t *testing.T) {
	Convey("test format date", t, func() {
		a, err := NewAction([]interface{}{newFormatDateAction().Name(), "$.test", "yyyy-mm-ddTHH:MM:SSZ", "yyyy-mm-dd HH:MM:SS"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "2022-11-15T15:41:25Z")
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "2022-11-15 15:41:25")
	})
	Convey("test format unix time", t, func() {
		a, err := NewAction([]interface{}{newFormatUnixTimeAction().Name(), "$.data.time", "yyyy-mm-ddTHH:MM:SSZ"})
		So(err, ShouldBeNil)
		ceCtx := &context.EventContext{
			Event: newEvent(),
			Data:  map[string]interface{}{"time": float64(1668498285)},
		}
		err = a.Execute(ceCtx)
		So(err, ShouldBeNil)
		So(ceCtx.Data.(map[string]interface{})["time"], ShouldEqual, "2022-11-15T07:44:45Z")
	})
}
