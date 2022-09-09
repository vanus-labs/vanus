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

func TestLogStream_SelectFile(t *testing.T) {
	Convey("log stream", t, func() {
		walDir, err := os.MkdirTemp("", "wal-*")
		So(err, ShouldBeNil)

		s := logStream{
			dir:       walDir,
			blockSize: defaultBlockSize,
			fileSize:  fileSize,
		}

		Convey("select file", func() {
			f := s.selectFile(0, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = s.selectFile(1, false)
			}, ShouldPanic)

			f0 := s.selectFile(0, true)
			So(f0, ShouldNotBeNil)
			So(s.stream, ShouldHaveLength, 1)

			f = s.selectFile(fileSize-1, false)
			So(f, ShouldEqual, f0)

			f = s.selectFile(fileSize, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = s.selectFile(fileSize+1, false)
			}, ShouldPanic)

			f1 := s.selectFile(fileSize, true)
			So(f1, ShouldNotBeNil)
			So(s.stream, ShouldHaveLength, 2)

			f = s.selectFile(fileSize*2-1, false)
			So(f, ShouldEqual, f1)

			f = s.selectFile(fileSize*2, false)
			So(f, ShouldBeNil)

			So(func() {
				_ = s.selectFile(fileSize*2+1, false)
			}, ShouldPanic)

			f = s.selectFile(fileSize-1, false)
			So(f, ShouldEqual, f0)

			So(func() {
				_ = s.selectFile(-1, false)
			}, ShouldPanic)
		})

		Reset(func() {
			s.Close(stdCtx.Background())

			err = os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})
}
