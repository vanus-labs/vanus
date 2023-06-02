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

package bytes

import (
	// standard libraries.
	"reflect"
	"testing"
	"unsafe"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestUnsafe(t *testing.T) {
	Convey("unsafe", t, func() {
		Convey("from string", func() {
			s := "hello"
			b := UnsafeFromString(s)
			strHdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
			byteHdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
			So(byteHdr.Data, ShouldEqual, strHdr.Data)
			So(byteHdr.Len, ShouldEqual, strHdr.Len)
			So(byteHdr.Cap, ShouldEqual, strHdr.Len)
		})

		Convey("to string", func() {
			b := []byte("hello")
			s := UnsafeToString(b)
			byteHdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
			strHdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
			So(strHdr.Data, ShouldEqual, byteHdr.Data)
			So(strHdr.Len, ShouldEqual, byteHdr.Len)
		})

		Convey("slice", func() {
			s := "hello"
			b := UnsafeSlice(s, 1, 4)
			strHdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
			byteHdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
			So(unsafe.Pointer(byteHdr.Data), ShouldEqual,
				unsafe.Add(unsafe.Pointer(strHdr.Data), 1))
			So(byteHdr.Len, ShouldEqual, 3)
			So(byteHdr.Cap, ShouldEqual, 3)
		})

		Convey("at", func() {
			s := "hello"
			for i := range s {
				c := UnsafeAt(s, i)
				So(c, ShouldEqual, s[i])
			}
		})
	})
}
