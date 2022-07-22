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

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	blockSize = 4 * 1024
)

func TestBlock(t *testing.T) {
	Convey("wal block", t, func() {
		b := Block{
			block: block{
				buf: make([]byte, blockSize),
			},
			SO: 4096,
		}

		Convey("properties", func() {
			So(b.Capacity(), ShouldEqual, blockSize)
			So(b.Size(), ShouldEqual, 0)
			So(b.Committed(), ShouldEqual, 0)
			So(b.Remaining(), ShouldEqual, blockSize)
			So(b.Full(), ShouldBeFalse)
			So(b.WriteOffset(), ShouldEqual, 4096)
		})

		Convey("append", func() {
			dataSize := blockSize - record.HeaderSize*2 + 1
			records := record.Pack(make([]byte, dataSize), dataSize+record.HeaderSize, 0)
			r := records[0]

			n, err := b.Append(r)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, r.Size())
			So(b.Full(), ShouldBeTrue)
			So(b.FullWithOff(n-1), ShouldBeFalse)
			So(b.Committed(), ShouldEqual, 0)
			So(b.WriteOffset(), ShouldEqual, 4096+b.Size())

			Convey("flush", func() {
				b.Flush(io.WriteAtFunc(func(b []byte, base int64, cb io.WriteCallback) {
					So(b[:n], ShouldResemble, r.Marshal())
					So(base, ShouldEqual, 0)

					cb(len(b), nil)
				}), n, 0, func(commit int64, err error) {
					So(err, ShouldBeNil)
					So(commit, ShouldEqual, n)
				})

				So(b.Committed(), ShouldEqual, n)

				b.Flush(nil, 0, 0, func(commit int64, err error) {
					So(err, ShouldBeNil)
					So(commit, ShouldEqual, n)
				})
			})
		})

		Convey("append with too large data", func() {
			dataSize := blockSize - record.HeaderSize + 1
			records := record.Pack(make([]byte, dataSize), dataSize+record.HeaderSize, 0)
			r := records[0]

			_, err := b.Append(r)

			So(err, ShouldNotBeNil)
		})
	})
}
