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
	stdJson "encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/block"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/errors"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/util"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
	. "github.com/smartystreets/goconvey/convey"
)

func TestEventlogManager_RunWithoutTask(t *testing.T) {
	Convey("test run", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli
		alloc := block.NewMockAllocator(ctrl)
		utMgr.allocator = alloc

		Convey("case: run without start", func() {
			alloc.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)

			el1 := metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}
			data1, _ := stdJson.Marshal(el1)
			el2 := metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}
			data2, _ := stdJson.Marshal(el2)
			el3 := metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}
			data3, _ := stdJson.Marshal(el3)

			elPairs := []kv.Pair{
				{
					Key:   filepath.Join(metadata.EventlogKeyPrefixInKVStore, el1.ID.String()),
					Value: data1,
				},
				{
					Key:   filepath.Join(metadata.EventlogKeyPrefixInKVStore, el2.ID.String()),
					Value: data2,
				},
				{
					Key:   filepath.Join(metadata.EventlogKeyPrefixInKVStore, el3.ID.String()),
					Value: data3,
				},
			}

			seg1 := new(struct {
				SegmentID vanus.ID `json:"segment_id"`
			})
			seg1.SegmentID = vanus.NewID()
			segData1, _ := stdJson.Marshal(seg1)

			seg2 := new(struct {
				SegmentID vanus.ID `json:"segment_id"`
			})
			seg2.SegmentID = vanus.NewID()
			segData2, _ := stdJson.Marshal(seg2)

			seg3 := new(struct {
				SegmentID vanus.ID `json:"segment_id"`
			})
			seg3.SegmentID = vanus.NewID()
			segData3, _ := stdJson.Marshal(seg3)
			segPairs := []kv.Pair{
				{
					Key:   filepath.Join(metadata.EventlogSegmentsKeyPrefixInKVStore, el3.ID.String(), seg1.SegmentID.Key()),
					Value: segData1,
				},
				{
					Key:   filepath.Join(metadata.EventlogSegmentsKeyPrefixInKVStore, el3.ID.String(), seg1.SegmentID.Key()),
					Value: segData2,
				},
				{
					Key:   filepath.Join(metadata.EventlogSegmentsKeyPrefixInKVStore, el3.ID.String(), seg1.SegmentID.Key()),
					Value: segData3,
				},
			}

			kvCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(4).DoAndReturn(func(
				ctx stdCtx.Context, path string) ([]kv.Pair, error) {
				if path == metadata.EventlogKeyPrefixInKVStore {
					return elPairs, nil
				}
				if path == filepath.Join(metadata.EventlogSegmentsKeyPrefixInKVStore, el3.ID.String()) {
					return segPairs, nil
				}
				return []kv.Pair{}, nil
			})

			segment1 := createTestSegment()
			segment1.ID = seg1.SegmentID
			_data1, _ := stdJson.Marshal(segment1)

			segment2 := createTestSegment()
			segment2.ID = seg2.SegmentID
			_data2, _ := stdJson.Marshal(segment2)

			segment3 := createTestSegment()
			segment3.ID = seg3.SegmentID
			_data3, _ := stdJson.Marshal(segment3)

			kvCli.EXPECT().Get(gomock.Any(), gomock.Any()).Times(3).DoAndReturn(func(
				ctx stdCtx.Context, path string) ([]byte, error) {
				if path == filepath.Join(metadata.SegmentKeyPrefixInKVStore, segment1.ID.String()) {
					return _data1, nil
				}
				if path == filepath.Join(metadata.SegmentKeyPrefixInKVStore, segment2.ID.String()) {
					return _data2, nil
				}
				if path == filepath.Join(metadata.SegmentKeyPrefixInKVStore, segment3.ID.String()) {
					return _data3, nil
				}
				return nil, nil
			})
			err := utMgr.Run(stdCtx.Background(), kvCli, false)
			So(err, ShouldBeNil)
			So(util.MapLen(&utMgr.eventLogMap), ShouldEqual, 3)
			v, exist := utMgr.eventLogMap.Load(el1.ID.Key())
			So(exist, ShouldBeTrue)
			So(v.(*eventlog).size(), ShouldEqual, 0)

			v, exist = utMgr.eventLogMap.Load(el2.ID.Key())
			So(exist, ShouldBeTrue)
			So(v.(*eventlog).size(), ShouldEqual, 0)

			v, exist = utMgr.eventLogMap.Load(el3.ID.Key())
			So(exist, ShouldBeTrue)
			So(v.(*eventlog).size(), ShouldEqual, 3)

			So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, 9)
		})
	})
}

func TestEventlogManager_RunWithTask(t *testing.T) {
	Convey("case: run with start", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		ctx := stdCtx.Background()
		kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		alloc := block.NewMockAllocator(ctrl)
		utMgr.allocator = alloc
		vol1 := metadata.VolumeMetadata{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024 * 1024,
		}
		alloc.EXPECT().Run(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)
		alloc.EXPECT().Pick(gomock.Any(), 3).AnyTimes().DoAndReturn(func(ctx stdCtx.Context, num int) ([]*metadata.Block, error) {
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
		volMgr.EXPECT().GetVolumeInstanceByID(vol1.ID).AnyTimes().Return(volIns)
		srv := server.NewMockServer(ctrl)
		volIns.EXPECT().GetServer().AnyTimes().Return(srv)
		volIns.EXPECT().Address().AnyTimes().Return("127.0.0.1:10001")
		grpcCli := segpb.NewMockSegmentServerClient(ctrl)
		srv.EXPECT().GetClient().AnyTimes().Return(grpcCli)
		grpcCli.EXPECT().ActivateSegment(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)

		utMgr.scaleTick = time.NewTicker(20 * time.Millisecond)
		utMgr.cleanTick = time.NewTicker(20 * time.Millisecond)
		kvCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return([]kv.Pair{}, nil)
		kvCli.EXPECT().Delete(gomock.Any(), gomock.Any()).Times(2).Return(nil)
		err := utMgr.Run(ctx, kvCli, true)
		So(err, ShouldBeNil)
		md := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}

		el, err := newEventlog(ctx, md, kvCli, false)
		So(err, ShouldBeNil)
		So(el.size(), ShouldEqual, 0)
		utMgr.eventLogMap.Store(el.md.ID.Key(), el)
		seg1 := createTestSegment()
		utMgr.segmentNeedBeClean.Store(seg1.ID.Key(), seg1)
		time.Sleep(100 * time.Millisecond)
		So(el.size(), ShouldEqual, 2)

		el2, err := newEventlog(ctx, md, kvCli, false)
		So(err, ShouldBeNil)
		So(el2.size(), ShouldEqual, 0)
		utMgr.eventLogMap.Store(el2.md.ID.Key(), el2)
		seg2 := createTestSegment()
		utMgr.segmentNeedBeClean.Store(seg2.ID.Key(), seg2)
		time.Sleep(100 * time.Millisecond)
		So(el.size(), ShouldEqual, 2)
		So(el2.size(), ShouldEqual, 2)

		utMgr.stop()
		el3, err := newEventlog(ctx, md, kvCli, false)
		So(err, ShouldBeNil)
		So(el3.size(), ShouldEqual, 0)
		utMgr.eventLogMap.Store(el3.md.ID.Key(), el3)
		seg3 := createTestSegment()
		utMgr.segmentNeedBeClean.Store(seg1.ID.Key(), seg3)
		time.Sleep(100 * time.Millisecond)
		So(el.size(), ShouldEqual, 2)
		So(el2.size(), ShouldEqual, 2)
		So(el3.size(), ShouldEqual, 0)
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
		kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(14).Return(nil)
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
			elog := utMgr.getEventLog(logMD.ID)
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

func TestEventlogManager_GetAppendableSegment(t *testing.T) {
	Convey("test GetAppendableSegment", t, func() {
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		mgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		mgr.kvClient = kvCli

		ctx := stdCtx.Background()
		kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(6).Return(nil)
		alloc := block.NewMockAllocator(ctrl)
		mgr.allocator = alloc
		vol1 := metadata.VolumeMetadata{
			ID:       vanus.NewID(),
			Capacity: 64 * 1024 * 1024 * 1024,
		}
		alloc.EXPECT().Pick(ctx, 3).Times(1).DoAndReturn(func(ctx stdCtx.Context, num int) ([]*metadata.Block, error) {
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
		volMgr.EXPECT().GetVolumeInstanceByID(vol1.ID).Times(4).Return(volIns)
		srv := server.NewMockServer(ctrl)
		volIns.EXPECT().GetServer().Times(1).Return(srv)
		volIns.EXPECT().Address().Times(3).Return("127.0.0.1:10001")
		grpcCli := segpb.NewMockSegmentServerClient(ctrl)
		srv.EXPECT().GetClient().Times(1).Return(grpcCli)
		grpcCli.EXPECT().ActivateSegment(ctx, gomock.Any()).Times(1).Return(nil, nil)

		Convey("case: no segment", func() {
			md := &metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}

			el, err := newEventlog(ctx, md, kvCli, false)
			So(err, ShouldBeNil)
			mgr.eventLogMap.Store(md.ID.Key(), el)
			segs, err := mgr.GetAppendableSegment(ctx, md, 1)
			So(err, ShouldBeNil)
			So(segs, ShouldHaveLength, 1)
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
	Convey("test eventlog operation", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		ctx := stdCtx.Background()
		md := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}
		el, _ := newEventlog(ctx, md, kvCli, false)

		seg1 := createTestSegment()
		seg2 := createTestSegment()
		seg3 := createTestSegment()
		seg4 := createTestSegment()

		kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(7).Return(nil)
		_ = el.add(ctx, seg1)
		_ = el.add(ctx, seg2)
		_ = el.add(ctx, seg3)
		_ = el.add(ctx, seg4)
		_ = el.add(ctx, seg1)
		seg1.Number = 1000
		seg1.State = StateFrozen
		seg2.Number = 1000
		seg2.State = StateFrozen
		seg3.Number = 900
		seg3.State = StateWorking
		seg4.Number = 0
		seg4.State = StateWorking

		Convey("case: add segments", func() {
			So(el.size(), ShouldEqual, 4)
		})

		Convey("case: get segment", func() {
			_seg := el.get(seg1.ID)
			So(_seg, ShouldEqual, seg1)
		})

		Convey("case: appendable segments related", func() {
			curSeg := el.currentAppendableSegment()
			So(curSeg, ShouldEqual, seg3)
			So(el.appendableSegmentNumber(), ShouldEqual, 2)
		})
		Convey("case: instructions", func() {
			So(el.head(), ShouldEqual, seg1)
			So(el.tail(), ShouldEqual, seg4)

			So(el.indexAt(0), ShouldEqual, seg1)
			So(el.indexAt(1), ShouldEqual, seg2)
			So(el.indexAt(2), ShouldEqual, seg3)
			So(el.indexAt(3), ShouldEqual, seg4)
			So(el.indexAt(4), ShouldBeNil)
			So(el.indexAt(999), ShouldBeNil)

			So(el.nextOf(el.indexAt(0)), ShouldEqual, seg2)
			So(el.nextOf(el.indexAt(1)), ShouldEqual, seg3)
			So(el.nextOf(el.indexAt(2)), ShouldEqual, seg4)
			So(el.nextOf(el.indexAt(3)), ShouldBeNil)

			So(el.previousOf(el.indexAt(0)), ShouldBeNil)
			So(el.previousOf(el.indexAt(1)), ShouldEqual, seg1)
			So(el.previousOf(el.indexAt(2)), ShouldEqual, seg2)
			So(el.previousOf(el.indexAt(3)), ShouldEqual, seg3)
		})
	})
}

func Test_ExpiredSegmentDeleting(t *testing.T) {
	Convey("test expired segment deleting", t, func() {
		Convey("", func() {

		})
	})
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
