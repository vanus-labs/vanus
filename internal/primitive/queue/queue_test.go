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

package queue

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestQueue(t *testing.T) {
	Convey("test queue", t, func() {
		q := New()
		q.Add("key1")
		So(q.Len(), ShouldEqual, 1)
		v, shutdown := q.Get()
		So(shutdown, ShouldBeFalse)
		So(v, ShouldEqual, "key1")
		So(q.Len(), ShouldEqual, 0)
		q.Done("key1")
	})
}

func TestQueueReAdd(t *testing.T) {
	Convey("test queue", t, func() {
		q := New()
		q.Add("key1")
		So(q.Len(), ShouldEqual, 1)
		v, shutdown := q.Get()
		So(shutdown, ShouldBeFalse)
		So(v, ShouldEqual, "key1")
		So(q.Len(), ShouldEqual, 0)
		q.ReAdd("key1")
		So(q.GetFailNum("key1"), ShouldEqual, 1)
		q.ClearFailNum("key1")
		So(q.GetFailNum("key1"), ShouldEqual, 0)
		v, shutdown = q.Get()
		So(shutdown, ShouldBeFalse)
		So(v, ShouldEqual, "key1")
		So(q.Len(), ShouldEqual, 0)
	})
}
