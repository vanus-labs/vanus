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
	"github.com/linkall-labs/vanus/internal/store/block"
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
	"github.com/linkall-labs/vanus/internal/store/vsb/codec"
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func TestVSBlock_Append(t *testing.T) {
	Convey("append context", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		ent := cetest.MakeStoredEntry0(ctrl)
		end := cetest.MakeStoredEndEntry(ctrl)

		dec, _ := codec.NewDecoder(false, codec.IndexSize)
		b := &vsBlock{
			actx: appendContext{
				offset: headerBlockSize,
			},
			enc: codec.NewEncoder(),
			dec: dec,
		}

		actx := b.NewAppendContext(nil)
		So(actx, ShouldNotBeNil)
		So(actx.WriteOffset(), ShouldEqual, headerBlockSize)
		So(actx.Archived(), ShouldBeFalse)

		frag := newFragment(vsbtest.EntryOffset0, []block.Entry{ent}, b.enc)
		actx = b.NewAppendContext(frag)
		So(actx, ShouldNotBeNil)
		So(actx.WriteOffset(), ShouldEqual, vsbtest.EntryOffset0+vsbtest.EntrySize0)
		So(actx.Archived(), ShouldBeFalse)

		frag = newFragment(vsbtest.EndEntryOffset, []block.Entry{end}, b.enc)
		actx = b.NewAppendContext(frag)
		So(actx, ShouldNotBeNil)
		So(actx.WriteOffset(), ShouldEqual, vsbtest.EndEntryOffset+vsbtest.EndEntrySize)
		So(actx.Archived(), ShouldBeTrue)

		b.actx.archived = 1
		actx = b.NewAppendContext(nil)
		So(actx, ShouldNotBeNil)
		So(actx.WriteOffset(), ShouldEqual, headerBlockSize)
		So(actx.Archived(), ShouldBeTrue)
	})

	Convey("append entries to vsb", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		ent0 := cetest.MakeEntry0(ctrl)
		ent1 := cetest.MakeEntry1(ctrl)

		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)

		dec, _ := codec.NewDecoder(false, codec.IndexSize)
		b := &vsBlock{
			capacity:   vsbtest.EntrySize0 + vsbtest.EntrySize1,
			dataOffset: headerBlockSize,
			actx: appendContext{
				offset: headerBlockSize,
			},
			enc: codec.NewEncoder(),
			dec: dec,
			f:   f,
		}

		Convey("append single entry, commit single fragment", func() {
			actx := b.NewAppendContext(nil)
			So(actx, ShouldNotBeNil)

			seqs, frag, full, err := b.PrepareAppend(context.Background(), actx, ent0)
			So(err, ShouldBeNil)
			So(seqs, ShouldResemble, []int64{0})
			So(frag.StartOffset(), ShouldEqual, headerBlockSize)
			So(frag.Size(), ShouldEqual, vsbtest.EntrySize0)
			So(full, ShouldBeFalse)

			stat := b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 0)
			So(stat.EntrySize, ShouldEqual, 0)

			archived, err := b.CommitAppend(context.Background(), frag)
			So(err, ShouldBeNil)
			So(archived, ShouldBeFalse)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 1)
			So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0)

			So(b.indexes, ShouldHaveLength, 1)
			idxtest.CheckIndex0(b.indexes[0], true)

			seqs, frag, full, err = b.PrepareAppend(context.Background(), actx, ent1)
			So(err, ShouldBeNil)
			So(seqs, ShouldResemble, []int64{1})
			So(frag.StartOffset(), ShouldEqual, headerBlockSize+vsbtest.EntrySize0)
			So(frag.Size(), ShouldEqual, vsbtest.EntrySize1)
			So(full, ShouldBeTrue)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 1)
			So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0)

			archived, err = b.CommitAppend(context.Background(), frag)
			So(err, ShouldBeNil)
			So(archived, ShouldBeFalse)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 2)
			So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

			So(b.indexes, ShouldHaveLength, 2)
			idxtest.CheckIndex0(b.indexes[0], true)
			idxtest.CheckIndex1(b.indexes[1], true)
		})

		Convey("append single entry, commit multiple fragments", func() {
			actx := b.NewAppendContext(nil)
			So(actx, ShouldNotBeNil)

			seqs, frag0, full, err := b.PrepareAppend(context.Background(), actx, ent0)
			So(err, ShouldBeNil)
			So(seqs, ShouldResemble, []int64{0})
			So(frag0.StartOffset(), ShouldEqual, headerBlockSize)
			So(frag0.Size(), ShouldEqual, vsbtest.EntrySize0)
			So(full, ShouldBeFalse)

			stat := b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 0)
			So(stat.EntrySize, ShouldEqual, 0)

			seqs, frag1, full, err := b.PrepareAppend(context.Background(), actx, ent1)
			So(err, ShouldBeNil)
			So(seqs, ShouldResemble, []int64{1})
			So(frag1.StartOffset(), ShouldEqual, headerBlockSize+vsbtest.EntrySize0)
			So(frag1.Size(), ShouldEqual, vsbtest.EntrySize1)
			So(full, ShouldBeTrue)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 0)
			So(stat.EntrySize, ShouldEqual, 0)

			archived, err := b.CommitAppend(context.Background(), frag0, frag1)
			So(err, ShouldBeNil)
			So(archived, ShouldBeFalse)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 2)
			So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

			So(b.indexes, ShouldHaveLength, 2)
			idxtest.CheckIndex0(b.indexes[0], true)
			idxtest.CheckIndex1(b.indexes[1], true)
		})

		Convey("append multiple entries, commit single fragment", func() {
			actx := b.NewAppendContext(nil)
			So(actx, ShouldNotBeNil)

			seqs, frag, full, err := b.PrepareAppend(context.Background(), actx, ent0, ent1)
			So(err, ShouldBeNil)
			So(seqs, ShouldResemble, []int64{0, 1})
			So(frag.StartOffset(), ShouldEqual, headerBlockSize)
			So(frag.Size(), ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)
			So(full, ShouldBeTrue)

			stat := b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 0)
			So(stat.EntrySize, ShouldEqual, 0)

			archived, err := b.CommitAppend(context.Background(), frag)
			So(err, ShouldBeNil)
			So(archived, ShouldBeFalse)

			stat = b.status()
			So(stat.Archived, ShouldBeFalse)
			So(stat.EntryNum, ShouldEqual, 2)
			So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

			So(b.indexes, ShouldHaveLength, 2)
			idxtest.CheckIndex0(b.indexes[0], true)
			idxtest.CheckIndex1(b.indexes[1], true)
		})

		Reset(func() {
			buf := make([]byte, vsbtest.EntrySize0+vsbtest.EntrySize1)
			_, err := f.ReadAt(buf, headerBlockSize)
			So(err, ShouldBeNil)

			n0, entry0, err := b.dec.Unmarshal(buf[:vsbtest.EntrySize0])
			So(err, ShouldBeNil)
			So(n0, ShouldEqual, vsbtest.EntrySize0)
			cetest.CheckEntry0(entry0, false, true)

			n1, entry1, err := b.dec.Unmarshal(buf[vsbtest.EntrySize0:])
			So(err, ShouldBeNil)
			So(n1, ShouldEqual, vsbtest.EntrySize1)
			cetest.CheckEntry1(entry1, false, true)

			err = f.Close()
			So(err, ShouldBeNil)
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		})
	})

	Convey("append vsb to archived", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		ent0 := cetest.MakeEntry0(ctrl)
		ent1 := cetest.MakeEntry1(ctrl)

		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)

		defer func() {
			err = f.Close()
			So(err, ShouldBeNil)
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		}()

		dec, _ := codec.NewDecoder(false, codec.IndexSize)
		b := &vsBlock{
			capacity:   vsbtest.EntrySize0 + vsbtest.EntrySize1,
			dataOffset: vsbtest.EntryOffset0,
			indexSize:  codec.IndexSize,
			actx: appendContext{
				offset: vsbtest.EntryOffset0,
			},
			enc: codec.NewEncoder(),
			dec: dec,
			f:   f,
		}

		actx := b.NewAppendContext(nil)
		So(actx, ShouldNotBeNil)

		seqs, frag0, full, err := b.PrepareAppend(context.Background(), actx, ent0, ent1)
		So(err, ShouldBeNil)
		So(seqs, ShouldResemble, []int64{0, 1})
		So(frag0.StartOffset(), ShouldEqual, vsbtest.EntryOffset0)
		So(frag0.Size(), ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)
		So(full, ShouldBeTrue)

		frag1, err := b.PrepareArchive(context.Background(), actx)
		So(err, ShouldBeNil)
		So(frag1.StartOffset(), ShouldEqual, vsbtest.EndEntryOffset)

		stat := b.status()
		So(stat.Archived, ShouldBeFalse)
		So(stat.EntryNum, ShouldEqual, 0)
		So(stat.EntrySize, ShouldEqual, 0)

		archived, err := b.CommitAppend(context.Background(), frag0, frag1)
		So(err, ShouldBeNil)
		So(archived, ShouldBeTrue)

		stat = b.status()
		So(stat.Archived, ShouldBeTrue)
		So(stat.EntryNum, ShouldEqual, 2)
		So(stat.EntrySize, ShouldEqual, vsbtest.EntrySize0+vsbtest.EntrySize1)

		So(b.indexes, ShouldHaveLength, 2)
		idxtest.CheckIndex0(b.indexes[0], true)
		idxtest.CheckIndex1(b.indexes[1], true)

		buf := make([]byte, vsbtest.EndEntrySize)
		_, err = f.ReadAt(buf, vsbtest.EndEntryOffset)
		So(err, ShouldBeNil)

		n, entry, err := b.dec.Unmarshal(buf)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, vsbtest.EndEntrySize)
		cetest.CheckEndEntry(entry, true)

		b.wg.Wait()

		buf = make([]byte, vsbtest.IndexEntrySize)
		_, err = f.ReadAt(buf, vsbtest.IndexEntryOffset)
		So(err, ShouldBeNil)

		n, entry, err = b.dec.Unmarshal(buf)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, vsbtest.IndexEntrySize)
		idxtest.CheckEntry(entry, true)

		buf = make([]byte, headerSize)
		_, err = f.ReadAt(buf, 0)
		So(err, ShouldBeNil)
		So(buf, ShouldResemble, vsbtest.ArchivedHeaderData)
	})
}
