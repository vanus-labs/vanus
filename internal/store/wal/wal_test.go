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
	"sync/atomic"
	"testing"
	"time"

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

			// Invoke callback of append data0, before append data1 return.
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

	Convey("flush wal when timeout", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		flushTimeout := time.Second
		wal, err := NewWAL(walDir, WithFileSize(fileSize), WithFlushTimeout(flushTimeout))
		So(err, ShouldBeNil)

		data := make([]byte, defaultBlockSize)

		startTime := time.Now()
		var t0, t1 time.Time
		wal.AppendOne(data0, WithCallback(func(_ Result) {
			t0 = time.Now()
		}))
		wal.AppendOne(data, WithCallback(func(_ Result) {
			t1 = time.Now()
		}))
		wal.AppendOne(data1).Wait()
		t2 := time.Now()

		So(t0, ShouldHappenBefore, startTime.Add(flushTimeout))
		So(t1, ShouldHappenAfter, startTime.Add(flushTimeout))
		So(t2, ShouldHappenAfter, t1)

		wal.Close()
		wal.Wait()

		err = os.RemoveAll(walDir)
		So(err, ShouldBeNil)
	})

	Convey("wal append one after close", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		wal, err := NewWAL(walDir, WithFileSize(fileSize))
		So(err, ShouldBeNil)

		var inflight int32 = 100
		for i := int32(0); i < inflight; i++ {
			wal.AppendOne(data0, WithCallback(func(_ Result) {
				atomic.AddInt32(&inflight, -1)
			}))
		}

		wal.Close()

		_, err = wal.AppendOne(data1).Wait()
		So(err, ShouldNotBeNil)

		wal.Wait()

		// NOTE: All appends are guaranteed to return before wal is closed.
		So(atomic.LoadInt32(&inflight), ShouldBeZeroValue)

		// NOTE: There is no guarantee that data0 will be successfully written.
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

		Convey("append without entry", func() {
			offsets, err := wal.Append([][]byte{}).Wait()

			So(err, ShouldBeNil)
			So(len(offsets), ShouldEqual, 0)
			So(wal.wb.Size(), ShouldEqual, 0)
			So(wal.wb.Committed(), ShouldEqual, 0)
		})

		Reset(func() {
			wal.Close()
			wal.Wait()

			err := os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})
}
