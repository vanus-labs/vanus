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

package io

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestWriteAtFunc(t *testing.T) {
	Convey("WriteAtFunc", t, func() {
		var f0 WriteAtFunc = func(b []byte, off int64, cb WriteCallback) {
			So(b, ShouldBeNil)
			So(off, ShouldEqual, 0)
			So(cb, ShouldBeNil)
		}
		var w0 WriterAt = f0
		w0.WriteAt(nil, 0, nil)

		buf := []byte{0x00}
		var callback WriteCallback = func(n int, err error) {}
		var f1 WriteAtFunc = func(b []byte, off int64, cb WriteCallback) {
			So(b, ShouldResemble, buf)
			So(off, ShouldEqual, 1)
			So(cb, ShouldEqual, callback)
		}
		var w1 WriterAt = f1
		w1.WriteAt(buf, 1, callback)
	})
}
