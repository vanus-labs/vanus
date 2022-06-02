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

package server

import (
	stdCtx "context"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
	"google.golang.org/grpc"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestVolumeInstance(t *testing.T) {
	Convey("test volume instance", t, func() {
		md := &metadata.VolumeMetadata{
			ID:       vanus.NewID(),
			Capacity: 32 * 1024 * 1024 * 1024,
			Used:     0,
			Blocks:   map[uint64]*metadata.Block{},
		}
		ins := NewInstance(md)
		So(ins.GetMeta(), ShouldEqual, md)
		So(ins.ID(), ShouldEqual, md.ID)
		So(ins.Address(), ShouldBeEmpty)
		So(ins.GetServer(), ShouldBeNil)

		ctrl := gomock.NewController(t)
		srv := NewMockServer(ctrl)

		srv.EXPECT().IsActive(stdCtx.Background()).Times(1).Return(false)
		srv.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(1234))
		srv.EXPECT().Address().AnyTimes().Return("127.0.0.1:10001")
		srv.EXPECT().Uptime().AnyTimes().Return(time.Now())
		ins.SetServer(srv)
		So(ins.GetServer(), ShouldBeNil)
		So(ins.Address(), ShouldBeEmpty)

		srv.EXPECT().IsActive(stdCtx.Background()).Times(2).Return(true)
		ins.SetServer(srv)
		So(ins.GetServer(), ShouldEqual, srv)
		So(ins.Address(), ShouldEqual, "127.0.0.1:10001")

		srv.EXPECT().Close().Times(1).Return(nil)
		So(ins.Close(), ShouldBeNil)

		segCli := segpb.NewMockSegmentServerClient(ctrl)
		srv.EXPECT().GetClient().AnyTimes().Return(segCli)
		ctx := stdCtx.Background()
		f := func(ctx stdCtx.Context, in *segpb.CreateBlockRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
			So(in.Size, ShouldEqual, 32*1024*1024)
			return &empty.Empty{}, nil
		}
		segCli.EXPECT().CreateBlock(ctx, gomock.Any(), gomock.Any()).Times(1).DoAndReturn(f)
		block, err := ins.CreateBlock(ctx, 32*1024*1024)
		So(err, ShouldBeNil)
		So(block.VolumeID, ShouldEqual, md.ID)
		So(block.Capacity, ShouldEqual, 32*1024*1024)
		So(block.Size, ShouldBeZeroValue)
		So(block.SegmentID, ShouldBeZeroValue)
		So(block.EventlogID, ShouldBeZeroValue)

		f = func(ctx stdCtx.Context, in *segpb.CreateBlockRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
			So(in.Size, ShouldEqual, 64*1024*1024)
			return &empty.Empty{}, nil
		}
		segCli.EXPECT().CreateBlock(ctx, gomock.Any(), gomock.Any()).Times(1).DoAndReturn(f)
		block2, err := ins.CreateBlock(ctx, 64*1024*1024)
		So(err, ShouldBeNil)

		So(md.Used, ShouldEqual, 96*1024*1024)
		So(md.Blocks[block.ID.Uint64()], ShouldEqual, block)
		So(md.Blocks[block2.ID.Uint64()], ShouldEqual, block2)

		f2 := func(ctx stdCtx.Context, in *segpb.RemoveBlockRequest, opts ...grpc.CallOption) (*empty.Empty, error) {
			So(in.Id, ShouldEqual, block.ID.Uint64())
			return &empty.Empty{}, nil
		}
		segCli.EXPECT().RemoveBlock(ctx, gomock.Any(), gomock.Any()).Times(1).DoAndReturn(f2)

		err = ins.DeleteBlock(ctx, block.ID)
		So(err, ShouldBeNil)
		So(md.Used, ShouldEqual, 64*1024*1024)
		So(md.Blocks[block.ID.Uint64()], ShouldBeNil)
		So(md.Blocks[block2.ID.Uint64()], ShouldEqual, block2)
	})
}
