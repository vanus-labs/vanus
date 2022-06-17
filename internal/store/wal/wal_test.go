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

package wal

import (
	// standard libraries.
	"fmt"
	"os"
	"path/filepath"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

var (
	fileSize int64 = 8 * defaultBlockSize
	data0          = []byte{0x41, 0x42, 0x43}
	data1          = []byte{0x44, 0x45, 0x46, 0x47}
)

func TestWAL_AppendOne(t *testing.T) {
	Convey("wal append one", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		wal, err := NewWAL(walDir, WithFileSize(fileSize))
		So(err, ShouldBeNil)

		Convey("append one with callback", func() {
			var done bool

			wal.AppendOne(data0, WithCallback(func(_ Result) {
				done = true
			}))
			n, _ := wal.AppendOne(data1).Wait()

			// Invoke callback of appand data0, before appand data1 return.
			So(done, ShouldBeTrue)
			So(n, ShouldEqual, 21)
			So(wal.wb.Size(), ShouldEqual, 21)
			So(wal.wb.Committed(), ShouldEqual, 21)

			filePath := filepath.Join(walDir, fmt.Sprintf("%020d.log", 0))
			data, err2 := os.ReadFile(filePath)
			So(err2, ShouldBeNil)

			So(data[:21+record.HeaderSize], ShouldResemble,
				[]byte{
					0x7D, 0x7F, 0xEB, 0x7A, 0x00, 0x03, 0x01, 0x41, 0x42, 0x43,
					0x52, 0x74, 0x2F, 0x51, 0x00, 0x04, 0x01, 0x44, 0x45, 0x46, 0x47,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				})
		})

		Convey("append one with large data", func() {
			data := make([]byte, fileSize)

			n, err := wal.AppendOne(data, WithoutBatching()).Wait()

			So(err, ShouldBeNil)
			So(n, ShouldEqual, fileSize+9*record.HeaderSize)
			So(len(wal.stream.stream), ShouldEqual, 2)
		})

		Reset(func() {
			wal.Close()
			wal.Wait()

			err := os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})

	Convey("wal append one after close", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		wal, err := NewWAL(walDir, WithFileSize(fileSize))
		So(err, ShouldBeNil)

		var done bool
		wal.AppendOne(data0, WithCallback(func(_ Result) {
			done = true
		}))

		wal.Close()
		wal.Wait()
		So(done, ShouldBeTrue)

		_, err = wal.AppendOne(data1).Wait()
		So(err, ShouldNotBeNil)

		// So(wal.wb.Size(), ShouldEqual, 10)
		// So(wal.wb.Committed(), ShouldEqual, 10)

		err = os.RemoveAll(walDir)
		So(err, ShouldBeNil)
	})
}

func TestWAL_Append(t *testing.T) {
	Convey("wal append", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		wal, err := NewWAL(walDir, WithFileSize(fileSize))
		So(err, ShouldBeNil)

		Convey("append without batching", func() {
			offsets, err := wal.Append([][]byte{
				data0, data1,
			}, WithoutBatching()).Wait()

			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 2)
			So(offsets[0], ShouldEqual, 10)
			So(offsets[1], ShouldEqual, 21)
			So(wal.wb.Size(), ShouldEqual, 21)
			So(wal.wb.Committed(), ShouldEqual, 21)

			filePath := filepath.Join(walDir, fmt.Sprintf("%020d.log", 0))
			data, err2 := os.ReadFile(filePath)
			So(err2, ShouldBeNil)

			So(data[:21+record.HeaderSize], ShouldResemble,
				[]byte{
					0x7D, 0x7F, 0xEB, 0x7A, 0x00, 0x03, 0x01, 0x41, 0x42, 0x43,
					0x52, 0x74, 0x2F, 0x51, 0x00, 0x04, 0x01, 0x44, 0x45, 0x46, 0x47,
					0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				})
		})

		Reset(func() {
			wal.Close()
			wal.Wait()

			err := os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})
}
