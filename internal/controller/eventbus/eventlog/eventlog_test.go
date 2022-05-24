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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	segpb "github.com/linkall-labs/vsproto/pkg/segment"
	. "github.com/smartystreets/goconvey/convey"
	"path/filepath"
	"testing"
	"time"
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
	Convey("test UpdateSegment", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		ctx := stdCtx.Background()
		Convey("case: the eventlog doesn't exist", func() {
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				"dont_exist": {
					{
						ID: vanus.NewID(),
					},
					{
						ID: vanus.NewID(),
					},
				},
			})
		})
		md := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}
		elog, err := newEventlog(ctx, md, kvCli, false)
		So(err, ShouldBeNil)
		Convey("case: test segment to be updated", func() {
			seg := createTestSegment()
			kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(6).Return(nil)
			err := elog.add(ctx, seg)
			So(err, ShouldBeNil)
			utMgr.eventLogMap.Store(md.ID.Key(), elog)
			// segment doesn't exist
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {
					{
						ID: vanus.NewID(),
					},
				},
			})
			// segment doesn't need to be updated
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {
					{
						ID: seg.ID,
					},
				},
			})

			// segment need to be updated
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {
					{
						ID:     seg.ID,
						Size:   1024,
						Number: 16,
						State:  StateWorking,
					},
				},
			})

			// mark is full
			_seg := createTestSegment()
			err = elog.add(ctx, _seg)
			So(err, ShouldBeNil)
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {
					{
						ID:    seg.ID,
						State: StateFrozen,
					},
				},
			})

		})
	})
}

func TestEventlogManager_UpdateSegmentReplicas(t *testing.T) {
	Convey("test UpdateSegmentReplicas", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		blk := &metadata.Block{
			ID:        vanus.NewID(),
			Capacity:  64 * 1024 * 1024,
			SegmentID: 0,
		}
		seg := createTestSegment()
		seg.Replicas.Term = 3
		utMgr.globalSegmentMap.Store(seg.ID.Key(), seg)
		utMgr.globalBlockMap.Store(blk.ID.Key(), blk)

		ctx := stdCtx.Background()
		kvCli.EXPECT().Set(ctx, filepath.Join(metadata.SegmentKeyPrefixInKVStore, seg.ID.String()),
			gomock.Any()).Times(1).Return(nil)

		err := utMgr.UpdateSegmentReplicas(ctx, vanus.NewID(), 3)
		So(err, ShouldEqual, errors.ErrBlockNotFound)

		err = utMgr.UpdateSegmentReplicas(ctx, blk.ID, 3)
		So(err, ShouldEqual, errors.ErrSegmentNotFound)

		blk.SegmentID = seg.ID
		err = utMgr.UpdateSegmentReplicas(ctx, blk.ID, 3)
		So(err, ShouldBeNil)

		err = utMgr.UpdateSegmentReplicas(ctx, blk.ID, 4)
		So(err, ShouldBeNil)
	})

}

func TestEventlog(t *testing.T) {

}

func createTestSegment() *Segment {
	leader := vanus.NewID()
	fo1 := vanus.NewID()
	fo2 := vanus.NewID()
	return &Segment{
		ID: vanus.NewID(),
		Replicas: &ReplicaGroup{
			ID:     vanus.NewID(),
			Leader: leader.Uint64(),
			Peers: map[uint64]*metadata.Block{
				leader.Uint64(): {
					ID: leader,
				},
				fo1.Uint64(): {
					ID: fo1,
				},
				fo2.Uint64(): {
					ID: fo2,
				},
			},
			Term:     0,
			CreateAt: time.Now(),
		},
	}
}
