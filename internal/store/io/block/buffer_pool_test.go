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

package block

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

var emptyBuf = make([]byte, bufferSize)

func TestBufferPool(t *testing.T) {
	Convey("wal block allocator", t, func() {
		pool := NewBufferPool(bufferSize)
		So(pool, ShouldNotBeNil)
		So(pool.BufferSize(), ShouldEqual, bufferSize)

		b0 := pool.Get(0)
		So(b0.Base(), ShouldEqual, 0)
		So(b0.Capacity(), ShouldEqual, bufferSize)
		So(b0.Size(), ShouldEqual, 0)
		So(b0.Committed(), ShouldEqual, 0)
		So(b0.fp, ShouldEqual, 0)
		So(b0.buf, ShouldResemble, emptyBuf)

		b1 := pool.Get(bufferSize)
		So(b1.Base(), ShouldEqual, bufferSize)
		So(b1.Capacity(), ShouldEqual, bufferSize)
		So(b1.Size(), ShouldEqual, 0)
		So(b1.Committed(), ShouldEqual, 0)
		So(b1.fp, ShouldEqual, 0)
		So(b1.buf, ShouldResemble, emptyBuf)

		pool.Put(b0)

		b2 := pool.Get(2 * bufferSize)
		So(b2.Base(), ShouldEqual, 2*bufferSize)
		So(b2.Capacity(), ShouldEqual, bufferSize)
		So(b2.Size(), ShouldEqual, 0)
		So(b2.Committed(), ShouldEqual, 0)
		So(b2.fp, ShouldEqual, 0)
		So(b2.buf, ShouldResemble, emptyBuf)
	})
}
