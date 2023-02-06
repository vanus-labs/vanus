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
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func TestVSBlock_Read(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()

	idx0 := idxtest.MakeIndex0(ctrl)
	idx1 := idxtest.MakeIndex1(ctrl)

	dataOffset := vsbtest.EntryOffset0

	Convey("read entries from block", t, func() {
		f, err := os.CreateTemp("", "*.vsb")
		So(err, ShouldBeNil)
		defer func() {
			err = f.Close()
			So(err, ShouldBeNil)
			err = os.Remove(f.Name())
			So(err, ShouldBeNil)
		}()

		_, err = f.WriteAt(vsbtest.EntryData0, vsbtest.EntryOffset0)
		So(err, ShouldBeNil)
		_, err = f.WriteAt(vsbtest.EntryData1, vsbtest.EntryOffset1)
		So(err, ShouldBeNil)

		dec, _ := codec.NewDecoder(false, codec.IndexSize)
		b := &vsBlock{
			dataOffset: dataOffset,
			actx: appendContext{
				offset: dataOffset,
			},
			indexes: []index.Index{idx0, idx1},
			dec:     dec,
			f:       f,
		}

		entries, err := b.Read(context.Background(), 0, 1)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 1)
		cetest.CheckEntry0(entries[0], false, false)

		entries, err = b.Read(context.Background(), 0, 3)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 2)
		cetest.CheckEntry0(entries[0], false, false)
		cetest.CheckEntry1(entries[1], false, false)

		entries, err = b.Read(context.Background(), 1, 2)
		So(err, ShouldBeNil)
		So(entries, ShouldHaveLength, 1)
		cetest.CheckEntry1(entries[0], false, false)

		_, err = b.Read(context.Background(), 2, 1)
		So(err, ShouldBeError, block.ErrOnEnd)

		_, err = b.Read(context.Background(), 3, 1)
		So(err, ShouldBeError, block.ErrExceeded)

		Convey("after block is full", func() {
			b.actx.archived = 1

			_, err = b.Read(context.Background(), 2, 1)
			So(err, ShouldBeError, block.ErrExceeded)
		})
	})
}
