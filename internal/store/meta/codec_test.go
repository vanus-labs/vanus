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

package meta

import (
	// standard libraries.
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestCodec_AppendValue(t *testing.T) {
	Convey("append deletedMark", t, func() {
		out, err := appendValue(nil, deletedMark)
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Deleted)}), ShouldBeTrue)
	})

	Convey("append bool(true)", t, func() {
		out, err := appendValue(nil, true)
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(True)}), ShouldBeTrue)
	})
	Convey("append bool(false)", t, func() {
		out, err := appendValue(nil, false)
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(False)}), ShouldBeTrue)
	})

	Convey("appent int(1)", t, func() {
		out, err := appendValue(nil, int(-1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Int), 0x01}), ShouldBeTrue)
	})
	Convey("appent int8(1)", t, func() {
		out, err := appendValue(nil, int8(-1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Int8), 0x01}), ShouldBeTrue)
	})
	Convey("appent int16(1)", t, func() {
		out, err := appendValue(nil, int16(-1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Int16), 0x01}), ShouldBeTrue)
	})
	Convey("appent int32(1)", t, func() {
		out, err := appendValue(nil, int32(-1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Int32), 0x01}), ShouldBeTrue)
	})
	Convey("appent int64(1)", t, func() {
		out, err := appendValue(nil, int64(-1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Int64), 0x01}), ShouldBeTrue)
	})

	Convey("appent uint(1)", t, func() {
		out, err := appendValue(nil, uint(1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Uint), 0x01}), ShouldBeTrue)
	})
	Convey("appent int8(1)", t, func() {
		out, err := appendValue(nil, uint8(1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Uint8), 0x01}), ShouldBeTrue)
	})
	Convey("appent int16(1)", t, func() {
		out, err := appendValue(nil, uint16(1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Uint16), 0x01}), ShouldBeTrue)
	})
	Convey("appent int32(1)", t, func() {
		out, err := appendValue(nil, uint32(1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Uint32), 0x01}), ShouldBeTrue)
	})
	Convey("appent int64(1)", t, func() {
		out, err := appendValue(nil, uint64(1))
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Uint64), 0x01}), ShouldBeTrue)
	})

	Convey("appent float32(1)", t, func() {
		out, err := appendValue(nil, float32(1.0))
		So(err, ShouldBeNil)
		expected := make([]byte, 5)
		expected[0] = byte(Float32)
		binary.LittleEndian.PutUint32(expected[1:], math.Float32bits(1.0))
		So(bytes.Equal(out, expected), ShouldBeTrue)
	})
	Convey("appent float64(1)", t, func() {
		out, err := appendValue(nil, float64(1))
		So(err, ShouldBeNil)
		expected := make([]byte, 9)
		expected[0] = byte(Float64)
		binary.LittleEndian.PutUint64(expected[1:], math.Float64bits(1.0))
		So(bytes.Equal(out, expected), ShouldBeTrue)
	})

	Convey("appent []byte(678)", t, func() {
		out, err := appendValue(nil, []byte{0x06, 0x07, 0x08})
		So(err, ShouldBeNil)
		So(bytes.Equal(out, []byte{byte(Bytes), 0x03, 0x06, 0x07, 0x08}), ShouldBeTrue)
	})
	Convey("appent string(hello world!)", t, func() {
		out, err := appendValue(nil, "hello world!")
		So(err, ShouldBeNil)
		So(bytes.Equal(out,
			[]byte{byte(String), 0x0c, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', '!'}),
			ShouldBeTrue)
	})
}
