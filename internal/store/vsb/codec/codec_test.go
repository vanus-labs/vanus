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

package codec

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func TestAlignment(t *testing.T) {
	Convey("alignment", t, func() {
		So(alignment(0), ShouldEqual, 0)
		So(alignment(1), ShouldEqual, 8)
		So(alignment(2), ShouldEqual, 8)
		So(alignment(3), ShouldEqual, 8)
		So(alignment(4), ShouldEqual, 8)
		So(alignment(5), ShouldEqual, 8)
		So(alignment(6), ShouldEqual, 8)
		So(alignment(7), ShouldEqual, 8)
		So(alignment(8), ShouldEqual, 8)
		So(alignment(9), ShouldEqual, 16)
		So(alignment(16), ShouldEqual, 16)
	})
}

func TestEntryEncoder(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()

	entry0 := cetest.MakeStoredEntry0(ctrl)
	entry1 := cetest.MakeStoredEntry1(ctrl)
	endEntry := cetest.MakeStoredEndEntry(ctrl)
	indexEntry := idxtest.MakeEntry(ctrl)

	Convey("make entry encoder", t, func() {
		enc := NewEncoder()
		So(enc, ShouldNotBeNil)

		Convey("entry size", func() {
			sz0 := enc.Size(entry0)
			So(sz0, ShouldEqual, vsbtest.EntrySize0)

			sz1 := enc.Size(entry1)
			So(sz1, ShouldEqual, vsbtest.EntrySize1)

			sz2 := enc.Size(endEntry)
			So(sz2, ShouldEqual, vsbtest.EndEntrySize)

			sz3 := enc.Size(indexEntry)
			So(sz3, ShouldEqual, vsbtest.IndexEntrySize)
		})

		Convey("marshal ce entry", func() {
			buf0 := make([]byte, vsbtest.EntrySize0)
			n0, err0 := enc.MarshalTo(entry0, buf0)
			So(err0, ShouldBeNil)
			So(n0, ShouldEqual, vsbtest.EntrySize0)
			So(buf0, ShouldResemble, vsbtest.EntryData0)

			buf1 := make([]byte, vsbtest.EntrySize1)
			n1, err1 := enc.MarshalTo(entry1, buf1)
			So(err1, ShouldBeNil)
			So(n1, ShouldEqual, vsbtest.EntrySize1)
			So(buf1, ShouldResemble, vsbtest.EntryData1)
		})

		Convey("marshal end entry", func() {
			buf := make([]byte, vsbtest.EndEntrySize)
			n, err := enc.MarshalTo(endEntry, buf)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, vsbtest.EndEntrySize)
			So(buf, ShouldResemble, vsbtest.EndEntryData)
		})

		Convey("marshal index entry", func() {
			buf := make([]byte, vsbtest.IndexEntrySize)
			n, err := enc.MarshalTo(indexEntry, buf)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, vsbtest.IndexEntrySize)
			So(buf, ShouldResemble, vsbtest.IndexEntryData)
		})
	})
}

func TestEntryDecoder(t *testing.T) {
	Convey("make entry decoder", t, func() {
		dec, err := NewDecoder(true, IndexSize)
		So(err, ShouldBeNil)

		Convey("unmarshal ce entry", func() {
			n0, entry0, err := dec.Unmarshal(vsbtest.EntryData0)
			So(err, ShouldBeNil)
			So(n0, ShouldEqual, vsbtest.EntrySize0)
			cetest.CheckEntry0(entry0, false, false)

			n1, entry1, err := dec.Unmarshal(vsbtest.EntryData1)
			So(err, ShouldBeNil)
			So(n1, ShouldEqual, vsbtest.EntrySize1)
			cetest.CheckEntry1(entry1, false, false)
		})

		Convey("unmarshal end entry", func() {
			n, entry, err := dec.Unmarshal(vsbtest.EndEntryData)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, vsbtest.EndEntrySize)
			cetest.CheckEndEntry(entry, false)
		})

		Convey("unmarshal index entry", func() {
			n, entry, err := dec.Unmarshal(vsbtest.IndexEntryData)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, vsbtest.IndexEntrySize)
			idxtest.CheckEntry(entry, false)
		})
	})
}
