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

package json

import (
	// standard libraries.
	stdbytes "bytes"
	"math"
	"runtime"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/lib/bytes"
)

func TestHelper(t *testing.T) {
	Convey("append in JSON string", t, func() {
		Convey("append nil", func() {
			null := appendInJSONString(nil, nil)
			So(string(null), ShouldEqual, "null")
		})

		Convey("append bool", func() {
			t := appendInJSONString(nil, true)
			So(string(t), ShouldEqual, "true")

			f := appendInJSONString(nil, false)
			So(string(f), ShouldEqual, "false")
		})

		Convey("append int", func() {
			i := appendInJSONString(nil, int(math.MaxInt))
			So(string(i), ShouldEqual, "9223372036854775807")

			n := appendInJSONString(nil, int(math.MinInt))
			So(string(n), ShouldEqual, "-9223372036854775808")

			i8 := appendInJSONString(nil, int8(math.MaxInt8))
			So(string(i8), ShouldEqual, "127")

			n8 := appendInJSONString(nil, int8(math.MinInt8))
			So(string(n8), ShouldEqual, "-128")

			i16 := appendInJSONString(nil, int16(math.MaxInt16))
			So(string(i16), ShouldEqual, "32767")

			n16 := appendInJSONString(nil, int16(math.MinInt16))
			So(string(n16), ShouldEqual, "-32768")

			i32 := appendInJSONString(nil, int32(math.MaxInt32))
			So(string(i32), ShouldEqual, "2147483647")

			n32 := appendInJSONString(nil, int32(math.MinInt32))
			So(string(n32), ShouldEqual, "-2147483648")

			i64 := appendInJSONString(nil, int64(math.MaxInt64))
			So(string(i64), ShouldEqual, "9223372036854775807")

			n64 := appendInJSONString(nil, int64(math.MinInt64))
			So(string(n64), ShouldEqual, "-9223372036854775808")

			u := appendInJSONString(nil, uint(math.MaxUint))
			So(string(u), ShouldEqual, "18446744073709551615")

			u8 := appendInJSONString(nil, uint8(math.MaxUint8))
			So(string(u8), ShouldEqual, "255")

			u16 := appendInJSONString(nil, uint16(math.MaxUint16))
			So(string(u16), ShouldEqual, "65535")

			u32 := appendInJSONString(nil, uint32(math.MaxUint32))
			So(string(u32), ShouldEqual, "4294967295")

			u64 := appendInJSONString(nil, uint64(math.MaxUint64))
			So(string(u64), ShouldEqual, "18446744073709551615")
		})

		Convey("append float", func() {
			f32 := appendInJSONString(nil, float32(math.MaxFloat32))
			So(string(f32), ShouldEqual, "3.4028235e+38") // "3.40282346638528859811704183484516925440e+38"

			n32 := appendInJSONString(nil, float32(math.SmallestNonzeroFloat32))
			So(string(n32), ShouldEqual, "1e-45") // "1.401298464324817070923729583289916131280e-45"

			f64 := appendInJSONString(nil, float64(math.MaxFloat64))
			So(string(f64), ShouldEqual, "1.7976931348623157e+308") // "1.79769313486231570814527423731704356798070e+308"

			n64 := appendInJSONString(nil, float64(math.SmallestNonzeroFloat64))
			So(string(n64), ShouldEqual, "5e-324") // "4.9406564584124654417656879286822137236505980e-324"
		})

		Convey("append string", func() {
			str := appendInJSONString(nil, "\"foo\"\n")
			So(string(str), ShouldEqual, `\"foo\"\n`)
		})
	})

	Convey("write in JSON string", t, func() {
		var buf stdbytes.Buffer

		Convey("write nil", func() {
			err := writeInJSONString(&buf, nil)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "null")
		})

		Convey("write bool", func() {
			err := writeInJSONString(&buf, true)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "true")

			buf.Reset()

			err = writeInJSONString(&buf, false)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "false")
		})

		Convey("write int", func() {
			err := writeInJSONString(&buf, int(math.MaxInt))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "9223372036854775807")

			buf.Reset()

			err = writeInJSONString(&buf, int(math.MinInt))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "-9223372036854775808")

			buf.Reset()

			err = writeInJSONString(&buf, int8(math.MaxInt8))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "127")

			buf.Reset()

			err = writeInJSONString(&buf, int8(math.MinInt8))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "-128")

			buf.Reset()

			err = writeInJSONString(&buf, int16(math.MaxInt16))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "32767")

			buf.Reset()

			err = writeInJSONString(&buf, int16(math.MinInt16))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "-32768")

			buf.Reset()

			err = writeInJSONString(&buf, int32(math.MaxInt32))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "2147483647")

			buf.Reset()

			err = writeInJSONString(&buf, int32(math.MinInt32))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "-2147483648")

			buf.Reset()

			err = writeInJSONString(&buf, int64(math.MaxInt64))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "9223372036854775807")

			buf.Reset()

			err = writeInJSONString(&buf, int64(math.MinInt64))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "-9223372036854775808")

			buf.Reset()

			err = writeInJSONString(&buf, uint(math.MaxUint))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "18446744073709551615")

			buf.Reset()

			err = writeInJSONString(&buf, uint8(math.MaxUint8))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "255")

			buf.Reset()

			err = writeInJSONString(&buf, uint16(math.MaxUint16))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "65535")

			buf.Reset()

			err = writeInJSONString(&buf, uint32(math.MaxUint32))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "4294967295")

			buf.Reset()

			err = writeInJSONString(&buf, uint64(math.MaxUint64))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "18446744073709551615")
		})

		Convey("write float", func() {
			err := writeInJSONString(&buf, float32(math.MaxFloat32))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "3.4028235e+38") // "3.40282346638528859811704183484516925440e+38"

			buf.Reset()

			err = writeInJSONString(&buf, float32(math.SmallestNonzeroFloat32))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "1e-45") // "1.401298464324817070923729583289916131280e-45"

			buf.Reset()

			err = writeInJSONString(&buf, float64(math.MaxFloat64))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "1.7976931348623157e+308") // "1.79769313486231570814527423731704356798070e+308"

			buf.Reset()

			err = writeInJSONString(&buf, float64(math.SmallestNonzeroFloat64))
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "5e-324") // "4.9406564584124654417656879286822137236505980e-324"
		})

		Convey("write string", func() {
			err := writeInJSONString(&buf, "\"foo\"\n")
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, `\"foo\"\n`)

			buf.Reset()

			err = writeInJSONString(&buf, "0123456789ABCDEF0123456789ABCDEF01234567")
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, `0123456789ABCDEF0123456789ABCDEF01234567`)
		})

		Convey("no allocation for small value", func() {
			defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

			var stats0, stats1 runtime.MemStats
			runtime.ReadMemStats(&stats0)

			_ = writeInJSONString(bytes.DummyWriter, "0123456789ABCDEF0123456789ABCDEF")

			runtime.ReadMemStats(&stats1)
			So(stats1.Mallocs-stats0.Mallocs, ShouldEqual, 0)
			So(stats1.HeapAlloc-stats0.HeapAlloc, ShouldEqual, 0)
		})
	})
}
