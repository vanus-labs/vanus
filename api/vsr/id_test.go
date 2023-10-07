// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package vsr

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestID(t *testing.T) {
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
		So(id1, ShouldEqual, NewIDFromUint64(256))

		id2, err := NewIDFromString("100")
		So(err, ShouldBeNil)
		So(id1, ShouldEqual, id2)

		id3, err := NewIDFromString("100a")
		So(err, ShouldBeNil)
		So(id3, ShouldEqual, NewIDFromUint64(4106))
		So(id1, ShouldNotEqual, id3)

		_, err = NewIDFromString("100afegex")
		So(err, ShouldNotBeNil)
	})

	Convey("test id other", t, func() {
		id1 := NewIDFromUint64(10213234320)
		So(id1.Uint64(), ShouldEqual, 10213234320)
		So(id1.Key(), ShouldEqual, "0000000260C19690")
		id2 := NewIDFromUint64(10213234320)
		So(id1.Equals(id2), ShouldBeTrue)
		So(EmptyID(), ShouldEqual, 0)
	})
}
