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

package segmentedfile

import (
	// standard libraries.
	"os"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

const fileSize = 32 * 1024

func TestSegmentedFile_SelectSegment(t *testing.T) {
	Convey("segmented file", t, func() {
		dir, err := os.MkdirTemp("", "sf-*")
		So(err, ShouldBeNil)

		sf, err := Open(dir, WithSegmentSize(fileSize))
		So(err, ShouldBeNil)

		Convey("select segment", func() {
			f := sf.SelectSegment(0, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = sf.SelectSegment(1, false)
			}, ShouldPanic)

			f0 := sf.SelectSegment(0, true)
			So(f0, ShouldNotBeNil)
			So(sf.segments, ShouldHaveLength, 1)

			f = sf.SelectSegment(fileSize-1, false)
			So(f, ShouldEqual, f0)

			f = sf.SelectSegment(fileSize, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = sf.SelectSegment(fileSize+1, false)
			}, ShouldPanic)

			f1 := sf.SelectSegment(fileSize, true)
			So(f1, ShouldNotBeNil)
			So(sf.segments, ShouldHaveLength, 2)

			f = sf.SelectSegment(fileSize*2-1, false)
			So(f, ShouldEqual, f1)

			f = sf.SelectSegment(fileSize*2, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = sf.SelectSegment(fileSize*2+1, false)
			}, ShouldPanic)

			f = sf.SelectSegment(fileSize-1, false)
			So(f, ShouldEqual, f0)

			So(func() {
				_ = sf.SelectSegment(-1, false)
			}, ShouldPanic)
		})

		Reset(func() {
			sf.Close()

			err = os.RemoveAll(dir)
			So(err, ShouldBeNil)
		})
	})
}
