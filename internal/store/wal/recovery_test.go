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
	stdCtx "context"
	// standard libraries.
	"os"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestOpen(t *testing.T) {
	Convey("open wal in recover mode", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		Convey("create empty wal", func() {
			var entryNum int
			wal, err := Open(stdCtx.Background(), walDir, WithRecoveryCallback(func(entry []byte, r Range) error {
				entryNum++
				return nil
			}), WithFileSize(fileSize))

			So(err, ShouldBeNil)
			So(wal, ShouldNotBeNil)
			So(entryNum, ShouldEqual, 0)

			wal.Close()
			wal.Wait()
		})

		Convey("recover wal", func() {
			wal, err := Open(stdCtx.Background(), walDir, WithFileSize(fileSize))
			So(err, ShouldBeNil)
			wal.AppendOne(stdCtx.Background(), data0).Wait()
			wal.AppendOne(stdCtx.Background(), data1).Wait()
			wal.Close()
			wal.Wait()

			Convey("recover entire wal", func() {
				entries := make([][]byte, 0, 2)
				wal, err = Open(stdCtx.Background(), walDir, WithRecoveryCallback(func(entry []byte, r Range) error {
					entries = append(entries, entry)
					return nil
				}), WithFileSize(fileSize))

				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 2)
				So(entries[0], ShouldResemble, data0)
				So(entries[1], ShouldResemble, data1)

				wal.Close()
				wal.Wait()
			})

			Convey("recover wal with compacted", func() {
				entries := make([][]byte, 0, 1)
				wal, err = Open(stdCtx.Background(), walDir, FromPosition(10), WithRecoveryCallback(func(entry []byte, r Range) error {
					entries = append(entries, entry)
					return nil
				}), WithFileSize(fileSize))

				So(err, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0], ShouldResemble, data1)

				wal.Close()
				wal.Wait()
			})
		})

		Convey("recover large data", func() {
			data := make([]byte, fileSize)

			wal, err := Open(stdCtx.Background(), walDir, WithFileSize(fileSize))
			So(err, ShouldBeNil)
			wal.AppendOne(stdCtx.Background(), data).Wait()
			wal.Close()
			wal.Wait()

			entries := make([][]byte, 0, 1)
			wal, err = Open(stdCtx.Background(), walDir, WithRecoveryCallback(func(entry []byte, r Range) error {
				entries = append(entries, entry)
				return nil
			}), WithFileSize(fileSize))
			So(err, ShouldBeNil)
			So(len(entries), ShouldEqual, 1)
			So(entries[0], ShouldResemble, data)

			wal.Close()
			wal.Wait()
		})

		Reset(func() {
			err := os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})
}
