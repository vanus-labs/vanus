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

package file

import (
	stdCtx "context"
	"math/rand"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBlock_Creation(t *testing.T) {
	Convey("test block creation", t, func() {
		rd := rand.New(rand.NewSource(time.Now().UnixNano()))
		capacity := (rd.Int63n(128) + 16) * 1024 * 1024
		id := vanus.NewID()
		blk, err := Create(stdCtx.Background(), "/tmp", id, capacity)
		defer func() {
			_ = blk.Close(stdCtx.Background())
		}()
		So(err, ShouldBeNil)

		So(blk.ID(), ShouldEqual, id)
		So(blk.Path(), ShouldEqual, resolvePath("/tmp", id))
		So(blk.Appendable(), ShouldBeTrue)
		So(blk.size(), ShouldEqual, 0)
		So(blk.remaining(0, 0), ShouldEqual, capacity-headerSize)
		So(blk.persistHeader(stdCtx.Background()), ShouldBeNil)
		So(blk.loadHeader(stdCtx.Background()), ShouldBeNil)
	})
}

func TestBlock_IndexHeader(t *testing.T) {
	Convey("test block index", t, func() {
		rd := rand.New(rand.NewSource(time.Now().UnixNano()))
		capacity := (rd.Int63n(128) + 16) * 1024 * 1024
		id := vanus.NewID()
		blk, _ := Create(stdCtx.Background(), "/tmp", id, capacity)
		blk.version = rd.Int31()
		// TODO(wenfeng): fix here
		blk.fo.Store(int64(rd.Int31()))
		blk.actx.num = rd.Uint32()

		err := blk.persistHeader(stdCtx.Background())
		So(err, ShouldBeNil)
		So(blk.Close(stdCtx.Background()), ShouldBeNil)

		blkNew, err := Open(stdCtx.Background(), blk.path)
		So(err, ShouldBeNil)
		err = blkNew.loadHeader(stdCtx.Background())
		So(err, ShouldBeNil)
		So(blkNew.version, ShouldEqual, blk.version)
		So(blkNew.cap, ShouldEqual, blk.cap)
		So(blkNew.size(), ShouldEqual, blk.size())
		So(blkNew.actx.num, ShouldEqual, blk.actx.num)
		So(blkNew.Close(stdCtx.Background()), ShouldBeNil)
	})
}
