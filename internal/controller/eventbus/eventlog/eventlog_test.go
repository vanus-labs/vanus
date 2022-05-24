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

package eventlog

import (
	stdCtx "context"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/block"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestEventlogManager_Run(t *testing.T) {
	Convey("", t, func() {
		//STOP
	})
}

func TestEventlogManager_CreateAndGetEventlog(t *testing.T) {
	Convey("test AcquireEventLog", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		ctx := stdCtx.Background()
		kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(14).DoAndReturn(func(ctx stdCtx.Context,
			key string, value []byte) error {
			return nil
		})
		alloc := block.NewMockAllocator(ctrl)
		utMgr.allocator = alloc
		vol1 := metadata.VolumeMetadata{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024 * 1024,
		}
		alloc.EXPECT().Pick(ctx, 3).Times(2).DoAndReturn(func(ctx stdCtx.Context, num int) ([]*metadata.Block, error) {
			return []*metadata.Block{
				{
					ID:       vanus.NewID(),
					Capacity: 64 * 1024 * 1024,
					VolumeID: vol1.ID,
				},
				{
					ID:       vanus.NewID(),
					Capacity: 64 * 1024 * 1024,
					VolumeID: vol1.ID,
				},
				{
					ID:       vanus.NewID(),
					Capacity: 64 * 1024 * 1024,
					VolumeID: vol1.ID,
				},
			}, nil
		})

		volIns := server.NewMockInstance(ctrl)
		volMgr.EXPECT().GetVolumeInstanceByID(vol1.ID).Times(8).Return(volIns)
		srv := server.NewMockServer(ctrl)
		volIns.EXPECT().GetServer().Times(2).Return(srv)
		volIns.EXPECT().Address().Times(6).Return("127.0.0.1:10001")
		grpcCli := segpb.NewMockSegmentServerClient(ctrl)
		srv.EXPECT().GetClient().Times(2).Return(grpcCli)
		grpcCli.EXPECT().ActivateSegment(ctx, gomock.Any()).Times(2).Return(nil, nil)

		eventbusID := vanus.NewID()
		logMD, err := utMgr.AcquireEventLog(ctx, eventbusID)
		Convey("validate metadata", func() {
			So(err, ShouldBeNil)
			So(logMD.EventbusID, ShouldEqual, eventbusID)
		})

		Convey("validate eventlog", func() {
			elog := utMgr.getEventLog(stdCtx.Background(), logMD.ID)
			So(elog, ShouldNotBeNil)
			So(elog.size(), ShouldEqual, 2)
			So(elog.appendableSegmentNumber(), ShouldEqual, 2)
		})

		Convey("test get eventlog", func() {
			newLog := utMgr.GetEventLog(ctx, logMD.ID)
			So(newLog, ShouldEqual, logMD)

			segments := utMgr.GetEventLogSegmentList(logMD.ID)
			So(segments, ShouldHaveLength, 2)

			segments2, err := utMgr.GetAppendableSegment(ctx, logMD, 3)
			So(err, ShouldBeNil)
			So(segments2, ShouldHaveLength, 2)
			So(segments2[0], ShouldResemble, segments[0])
			So(segments2[1], ShouldResemble, segments[1])

			blockObj := utMgr.GetBlock(vanus.NewIDFromUint64(segments[0].Replicas.Leader))
			So(blockObj, ShouldNotBeNil)
			So(blockObj.EventlogID, ShouldEqual, logMD.ID)
			So(blockObj.VolumeID, ShouldEqual, vol1.ID)
			So(blockObj.SegmentID, ShouldEqual, segments[0].ID)

			seg := utMgr.GetSegment(segments2[1].ID)
			So(seg, ShouldNotBeNil)
			So(seg.EventLogID, ShouldEqual, logMD.ID)

			blockObj = utMgr.GetBlock(vanus.NewIDFromUint64(segments[1].Replicas.Leader))
			segAnother, err := utMgr.GetSegmentByBlockID(blockObj)
			So(err, ShouldBeNil)
			So(segAnother, ShouldEqual, seg)
		})
	})
}

func TestEventlogManager_UpdateSegment(t *testing.T) {

}

func TestEventlogManager_UpdateSegmentReplicas(t *testing.T) {

}

func TestEventlog(t *testing.T) {

}
