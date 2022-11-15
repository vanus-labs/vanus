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

	. "github.com/smartystreets/goconvey/convey"
)

func TestRegexAction(t *testing.T) {
	Convey("test replace with regex", t, func() {
		a, err := NewAction([]interface{}{newReplaceWithRegexAction().Name(), "$.test", "a", "value"})
		So(err, ShouldBeNil)
		e := newEvent()
		e.SetExtension("test", "a-a")
		err = a.Execute(&context.EventContext{
			Event: e,
		})
		So(err, ShouldBeNil)
		So(e.Extensions()["test"], ShouldEqual, "value-value")
	})
}
