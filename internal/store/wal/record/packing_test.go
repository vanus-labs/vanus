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

package record

import (
	// standard libraries.
	"bytes"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

const (
	blockSize = 4 * 1024
)

func TestPack(t *testing.T) {
	Convey("pack with enough space in first block", t, func() {
		records := Pack(rawData, blockSize, blockSize)
		So(len(records), ShouldEqual, 1)
		r0 := &records[0]
		So(r0.Type, ShouldEqual, Full)
		So(r0.Length, ShouldEqual, len(rawData))
		So(bytes.Equal(r0.Data, rawData), ShouldBeTrue)
	})

	Convey("pack with not enough space in first block", t, func() {
		records := Pack(rawData, HeaderSize+1, blockSize)
		So(len(records), ShouldEqual, 2)
		r0 := &records[0]
		So(r0.Type, ShouldEqual, First)
		So(r0.Length, ShouldEqual, 1)
		So(bytes.Equal(r0.Data, rawData[:1]), ShouldBeTrue)
		r1 := &records[1]
		So(r1.Type, ShouldEqual, Last)
		So(r1.Length, ShouldEqual, len(rawData)-1)
		So(bytes.Equal(r1.Data, rawData[1:]), ShouldBeTrue)
	})

	Convey("pack with no space in first block", t, func() {
		records := Pack(rawData, HeaderSize, blockSize)
		So(len(records), ShouldEqual, 2)
		r0 := &records[0]
		So(r0.Type, ShouldEqual, First)
		So(r0.Length, ShouldEqual, 0)
		So(bytes.Equal(r0.Data, []byte{}), ShouldBeTrue)
		r1 := &records[1]
		So(r1.Type, ShouldEqual, Last)
		So(r1.Length, ShouldEqual, len(rawData))
		So(bytes.Equal(r1.Data, rawData), ShouldBeTrue)
	})

	Convey("pack big data", t, func() {
		str := ""
		for i := 0; i < 2*blockSize; i++ {
			str += string("a"[0] + byte(i%26))
		}
		bigData := []byte(str)

		records := Pack(bigData, blockSize, blockSize)
		So(len(records), ShouldEqual, 3)
		r0 := &records[0]
		So(r0.Type, ShouldEqual, First)
		So(r0.Length, ShouldEqual, blockSize-HeaderSize)
		So(bytes.Equal(r0.Data, bigData[:r0.Length]), ShouldBeTrue)
		r1 := &records[1]
		So(r1.Type, ShouldEqual, Middle)
		So(r1.Length, ShouldEqual, blockSize-HeaderSize)
		So(bytes.Equal(r1.Data, bigData[r0.Length:r0.Length+r1.Length]), ShouldBeTrue)
		r2 := &records[2]
		So(r2.Type, ShouldEqual, Last)
		So(r2.Length, ShouldEqual, 2*HeaderSize)
		So(bytes.Equal(r2.Data, bigData[2*(blockSize-HeaderSize):]), ShouldBeTrue)
	})
}
