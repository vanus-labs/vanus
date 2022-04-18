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
	"context"
	"sync"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func doAppend(wal *WAL, entry []byte, wg *sync.WaitGroup) {
	entries := [][]byte{entry}
	_ = wal.Append(entries)
	// So(err, ShouldBeNil)
	wg.Done()
}

func TestWAL_Append(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wal := NewWAL(ctx)

	Convey("wal append testing", t, func() {
		wg := sync.WaitGroup{}
		wg.Add(2)

		go doAppend(wal, []byte{0x41, 0x42, 0x43}, &wg)
		go doAppend(wal, []byte{0x44, 0x45, 0x46, 0x47}, &wg)

		wg.Wait()

		So(wal.wb.wp, ShouldEqual, 21)
		So(wal.wb.fp, ShouldEqual, 21)
	})

	cancel()
	wal.Wait()
}
