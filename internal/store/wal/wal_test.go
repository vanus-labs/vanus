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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

func TestWAL_AppendOne(t *testing.T) {
	walDir, err := os.MkdirTemp("", "wal-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(walDir)

	wal, _ := newWAL(&logStream{dir: walDir}, 0)
	defer func() {
		wal.Close()
		wal.Wait()
	}()

	Convey("wal append testing", t, func() {
		wg := sync.WaitGroup{}
		wg.Add(1)

		wal.AppendOne([]byte{0x41, 0x42, 0x43}, WithCallback(func(_ Result) {
			wg.Done()
		}))
		wal.AppendOne([]byte{0x44, 0x45, 0x46, 0x47}).Wait()

		wg.Wait()

		So(wal.wb.wp, ShouldEqual, 21)
		So(wal.wb.fp, ShouldEqual, 21)

		filePath := filepath.Join(walDir, fmt.Sprintf("%020d.log", 0))
		data, err2 := os.ReadFile(filePath)
		So(err2, ShouldBeNil)

		So(bytes.Equal(data[:21+record.HeaderSize], []byte{
			0x52, 0x74, 0x2F, 0x51, 0x00, 0x04, 0x01, 0x44, 0x45, 0x46, 0x47,
			0x7D, 0x7F, 0xEB, 0x7A, 0x00, 0x03, 0x01, 0x41, 0x42, 0x43,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}) || bytes.Equal(data[:21+record.HeaderSize], []byte{
			0x7D, 0x7F, 0xEB, 0x7A, 0x00, 0x03, 0x01, 0x41, 0x42, 0x43,
			0x52, 0x74, 0x2F, 0x51, 0x00, 0x04, 0x01, 0x44, 0x45, 0x46, 0x47,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		}), ShouldBeTrue)
	})
}
