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

package vsb

import (
	// standard libraries.
	"context"
	"os"
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func TestVSBlock_Open(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()

	Convey("open archived vsb", t, func() {
		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)

		defer func() {
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		}()

		_, err = f.WriteAt(vsbtest.ArchivedHeaderData, 0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData0, vsbtest.EntryOffset0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData1, vsbtest.EntryOffset1)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EndEntryData, vsbtest.EndEntryOffset)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.IndexEntryData, vsbtest.IndexEntryOffset)
		So(err, ShouldBeNil)

		err = f.Close()
		So(err, ShouldBeNil)

		b := &vsBlock{
			path: f.Name(),
		}

		err = b.Open(context.Background())
		So(err, ShouldBeNil)

		stat := b.status()
		So(stat.Capacity, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)
		So(stat.Archived, ShouldBeTrue)
		So(stat.EntryNum, ShouldEqual, 2)
		So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

		So(b.indexes, ShouldHaveLength, 2)
		idxtest.CheckIndex0(b.indexes[0], false)
		idxtest.CheckIndex1(b.indexes[1], false)
	})

	Convey("open end vsb", t, func() {
		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)

		defer func() {
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		}()

		_, err = f.WriteAt(vsbtest.EmptyHeaderData, 0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData0, vsbtest.EntryOffset0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData1, vsbtest.EntryOffset1)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EndEntryData, vsbtest.EndEntryOffset)
		So(err, ShouldBeNil)

		err = f.Close()
		So(err, ShouldBeNil)

		b := &vsBlock{
			path: f.Name(),
		}

		err = b.Open(context.Background())
		So(err, ShouldBeNil)

		stat := b.status()
		So(stat.Capacity, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)
		So(stat.Archived, ShouldBeTrue)
		So(stat.EntryNum, ShouldEqual, 2)
		So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

		So(b.indexes, ShouldHaveLength, 2)
		idxtest.CheckIndex0(b.indexes[0], false)
		idxtest.CheckIndex1(b.indexes[1], false)
	})

	Convey("open working vsb", t, func() {
		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)

		defer func() {
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		}()

		_, err = f.WriteAt(vsbtest.EmptyHeaderData, 0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData0, vsbtest.EntryOffset0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData1, vsbtest.EntryOffset1)
		So(err, ShouldBeNil)

		err = f.Close()
		So(err, ShouldBeNil)

		b := &vsBlock{
			path: f.Name(),
		}

		err = b.Open(context.Background())
		So(err, ShouldBeNil)

		stat := b.status()
		So(stat.Capacity, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)
		So(stat.Archived, ShouldBeFalse)
		So(stat.EntryNum, ShouldEqual, 2)
		So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

		So(b.indexes, ShouldHaveLength, 2)
		idxtest.CheckIndex0(b.indexes[0], false)
		idxtest.CheckIndex1(b.indexes[1], false)
	})
}
