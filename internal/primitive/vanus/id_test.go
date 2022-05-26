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

package vanus

import (
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewID(t *testing.T) {
	Convey("test new id not equal", t, func() {
		var id1, id2 ID
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			id1 = NewID()
		}()
		go func() {
			defer wg.Done()
			id2 = NewID()
		}()
		wg.Wait()
		So(id1, ShouldNotEqual, id2)
	})

	Convey("test new id form uint64", t, func() {
		id1 := NewIDFromUint64(100)
		id2 := NewIDFromUint64(100)
		id3 := NewIDFromUint64(1000)
		So(id1, ShouldEqual, id2)
		So(id1, ShouldNotEqual, id3)
	})

	Convey("test new id form string", t, func() {
		id1, err := NewIDFromString("100")
		So(err, ShouldBeNil)
		So(id1, ShouldEqual, NewIDFromUint64(100))
		id2, err := NewIDFromString("100")
		So(err, ShouldBeNil)
		id3, err := NewIDFromString("1000")
		So(err, ShouldBeNil)
		So(id1, ShouldEqual, id2)
		So(id1, ShouldNotEqual, id3)
		_, err = NewIDFromString("100a")
		So(err, ShouldNotBeNil)
	})

	Convey("test id other", t, func() {
		id1 := NewIDFromUint64(100)
		So(id1.Uint64(), ShouldEqual, 100)
		So(id1.Key(), ShouldEqual, "100")
		id2 := NewIDFromUint64(100)
		So(id1.Equals(id2), ShouldBeTrue)
		So(EmptyID(), ShouldEqual, 0)
	})
}
