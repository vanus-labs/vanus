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

package segment

import (
	stdCtx "context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block/file"
	"github.com/linkall-labs/vanus/internal/store/block/replica"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/proto/pkg/errors"
	"os"
	"testing"

	"github.com/linkall-labs/vanus/internal/store"
	. "github.com/smartystreets/goconvey/convey"
)

func TestServer_RemoveBlock(t *testing.T) {
	Convey("test RemoveBlock", t, func() {
		cfg := store.Config{}
		srv := NewServer(cfg).(*server)
		ctx := stdCtx.Background()
		Convey("test state checking", func() {
			err := srv.RemoveBlock(ctx, vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "service state error")
			So(et.Code, ShouldEqual, errors.ErrorCode_SERVICE_NOT_RUNNING)
			So(et.Message, ShouldEqual, fmt.Sprintf("the server isn't ready to work, current state: %s",
				primitive.ServerStateCreated))
		})

		srv.state = primitive.ServerStateRunning
		Convey("the replica not found", func() {
			err := srv.RemoveBlock(ctx, vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Message, ShouldEqual, "the replica not found")
		})

		ctrl := gomock.NewController(t)
		_ = os.MkdirAll("/tmp/vanus/test/store/test", 0777)
		blk, err := file.Create(stdCtx.Background(), "/tmp/vanus/test/store/test", vanus.NewID(), 1024*1024*64)
		So(err, ShouldBeNil)

		Convey("the block not found", func() {
			rep := replica.NewMockReplica(ctrl)
			srv.writers.Store(blk.ID(), rep)
			srv.readers.Store(blk.ID(), blk)
			So(util.MapLen(&srv.writers), ShouldEqual, 1)
			So(util.MapLen(&srv.readers), ShouldEqual, 1)

			rep.EXPECT().Stop(ctx).Times(1)
			err := srv.RemoveBlock(ctx, blk.ID())
			So(util.MapLen(&srv.writers), ShouldEqual, 0)
			So(util.MapLen(&srv.readers), ShouldEqual, 0)
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Message, ShouldEqual, "the block not found")
		})

		Convey("test remove success", func() {
			rep := replica.NewMockReplica(ctrl)
			srv.blocks.Store(blk.ID(), blk)
			srv.writers.Store(blk.ID(), rep)
			srv.readers.Store(blk.ID(), blk)
			So(util.MapLen(&srv.blocks), ShouldEqual, 1)
			So(util.MapLen(&srv.writers), ShouldEqual, 1)
			So(util.MapLen(&srv.readers), ShouldEqual, 1)

			rep.EXPECT().Stop(ctx).Times(1)
			err := srv.RemoveBlock(ctx, blk.ID())
			So(util.MapLen(&srv.blocks), ShouldEqual, 0)
			So(util.MapLen(&srv.writers), ShouldEqual, 0)
			So(util.MapLen(&srv.readers), ShouldEqual, 0)
			So(err, ShouldBeNil)
			_, err = os.Open(blk.Path())
			So(os.IsNotExist(err), ShouldBeTrue)
		})
	})
}
