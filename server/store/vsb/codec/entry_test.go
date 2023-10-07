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

package codec

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestOffsetAndLength(t *testing.T) {
	Convey("offset and length", t, func() {
		off0, len0 := offsetAndLength([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
		So(off0, ShouldEqual, uint32(0))
		So(len0, ShouldEqual, uint32(0))

		off1, len1 := offsetAndLength([]byte{0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00})
		So(off1, ShouldEqual, uint32(0))
		So(len1, ShouldEqual, uint32(0x04030201))

		off2, len2 := offsetAndLength([]byte{0x00, 0x00, 0x00, 0x00, 0x05, 0x06, 0x07, 0x08})
		So(off2, ShouldEqual, uint32(0x08070605))
		So(len2, ShouldEqual, uint32(0))

		off3, len3 := offsetAndLength([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
		So(off3, ShouldEqual, uint32(0x08070605))
		So(len3, ShouldEqual, uint32(0x04030201))
	})
}

func TestValueIndex(t *testing.T) {
	Convey("value index", t, func() {
		bitmap := uint64(0b001011010010)
		So(valueIndex(bitmap, 0x01<<0), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<1), ShouldEqual, 0)
		So(valueIndex(bitmap, 0x01<<2), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<3), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<4), ShouldEqual, 1)
		So(valueIndex(bitmap, 0x01<<5), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<6), ShouldEqual, 2)
		So(valueIndex(bitmap, 0x01<<7), ShouldEqual, 3)
		So(valueIndex(bitmap, 0x01<<8), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<9), ShouldEqual, 4)
		So(valueIndex(bitmap, 0x01<<10), ShouldEqual, -1)
		So(valueIndex(bitmap, 0x01<<11), ShouldEqual, -1)
	})
}
