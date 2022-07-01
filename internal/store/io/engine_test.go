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
	"os"
	"sync"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

var (
	data0 = []byte{0x01, 0x02, 0x03, 0x04}
	data1 = []byte{0x05, 0x06, 0x07}
)

func doEngineTest(e Engine, f *os.File) {
	wg := sync.WaitGroup{}

	var rn int
	var rerr error

	wg.Add(1)
	e.WriteAt(f, data0, 0, func(n int, err error) {
		rn = n
		rerr = err
		wg.Done()
	})
	wg.Wait()

	So(rerr, ShouldBeNil)
	So(rn, ShouldEqual, len(data0))

	wg.Add(1)
	e.WriteAt(f, data1, 0, func(n int, err error) {
		rn = n
		rerr = err
		wg.Done()
	})
	wg.Wait()

	So(rerr, ShouldBeNil)
	So(rn, ShouldEqual, len(data1))

	buf := make([]byte, 4)
	n, err := f.ReadAt(buf, 0)

	So(err, ShouldBeNil)
	So(n, ShouldEqual, len(buf))
	So(buf, ShouldResemble, []byte{0x05, 0x06, 0x07, 0x04})
}

func TestEngine(t *testing.T) {
	f, err := os.CreateTemp("", "wal-engine-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	e := NewEngine()
	defer e.Close()

	Convey("engine", t, func() {
		doEngineTest(e, f)
	})
}
