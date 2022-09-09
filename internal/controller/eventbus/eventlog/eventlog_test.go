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
	"math"
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
	"github.com/linkall-labs/vanus/pkg/util"
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

			segment1 := createTestSegment(vanus.NewID())
			segment1.ID = seg1.SegmentID
			_data1, _ := stdJson.Marshal(segment1)

			segment2 := createTestSegment(vanus.NewID())
			segment2.ID = seg2.SegmentID
			_data2, _ := stdJson.Marshal(segment2)

			segment3 := createTestSegment(vanus.NewID())
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

func TestEventlogManager_ScaleSegmentTask(t *testing.T) {
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

		utMgr.scaleInterval = 5 * time.Millisecond
		// suspend those tasks
		utMgr.cleanInterval = time.Hour
		utMgr.checkSegmentExpiredInterval = time.Hour
		kvCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return([]kv.Pair{}, nil)
		err := utMgr.Run(ctx, kvCli, true)
		So(err, ShouldBeNil)
		md1 := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}
		md2 := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}

		// Test allocate segment automatically
		el, err := newEventlog(ctx, md1, kvCli, false)
		So(err, ShouldBeNil)
		So(el.size(), ShouldEqual, 0)
		utMgr.eventLogMap.Store(el.md.ID.Key(), el)
		time.Sleep(50 * time.Millisecond)
		So(el.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.eventLogMap), ShouldEqual, 1)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, defaultAppendableSegmentNumber*3)

		el2, err := newEventlog(ctx, md2, kvCli, false)
		So(err, ShouldBeNil)
		So(el2.size(), ShouldEqual, 0)
		utMgr.eventLogMap.Store(el2.md.ID.Key(), el2)
		So(util.MapLen(&utMgr.eventLogMap), ShouldEqual, 2)
		time.Sleep(50 * time.Millisecond)
		So(el.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(el2.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber*2)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, defaultAppendableSegmentNumber*3*2)

		head := el.head()
		head.State = StateFrozen
		t.Log(head.ID.Key())
		So(el.appendableSegmentNumber(), ShouldEqual, defaultAppendableSegmentNumber-1)
		So(util.MapLen(&utMgr.eventLogMap), ShouldEqual, 2)
		time.Sleep(50 * time.Millisecond)
		So(el.size(), ShouldEqual, defaultAppendableSegmentNumber+1)
		So(el2.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber*2+1)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, (defaultAppendableSegmentNumber*2+1)*3)

		utMgr.stop()
		head = el2.head()
		head.State = StateFrozen
		So(err, ShouldBeNil)
		time.Sleep(50 * time.Millisecond)
		So(el.size(), ShouldEqual, defaultAppendableSegmentNumber+1)
		So(el2.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber*2+1)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, (defaultAppendableSegmentNumber*2+1)*3)
	})
}

func TestEventlogManager_CleanSegmentTask(t *testing.T) {
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

		utMgr.scaleInterval = 5 * time.Millisecond
		utMgr.cleanInterval = 5 * time.Millisecond
		utMgr.checkSegmentExpiredInterval = time.Hour
		kvCli.EXPECT().List(gomock.Any(), gomock.Any()).Times(1).Return([]kv.Pair{}, nil)
		err := utMgr.Run(ctx, kvCli, true)
		So(err, ShouldBeNil)
		md1 := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}
		md2 := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}

		// Test allocate segment automatically
		el1, err := newEventlog(ctx, md1, kvCli, false)
		So(err, ShouldBeNil)
		el2, err := newEventlog(ctx, md2, kvCli, false)
		So(err, ShouldBeNil)
		utMgr.eventLogMap.Store(el1.md.ID.Key(), el1)
		utMgr.eventLogMap.Store(el2.md.ID.Key(), el2)
		time.Sleep(50 * time.Millisecond)
		So(el1.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(el2.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber*2)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, defaultAppendableSegmentNumber*3*2)

		head := el1.head()
		kvCli.EXPECT().Delete(gomock.Any(), metadata.GetSegmentMetadataKey(head.ID)).Times(1).Return(nil)
		kvCli.EXPECT().Delete(gomock.Any(), metadata.GetEventlogSegmentsMetadataKey(el1.md.ID, head.ID)).Times(1).Return(nil)
		for _, v := range head.Replicas.Peers {
			kvCli.EXPECT().Delete(gomock.Any(), metadata.GetBlockMetadataKey(v.VolumeID, v.ID)).Times(1).Return(nil)
			volIns.EXPECT().DeleteBlock(gomock.Any(), v.ID).Times(1).Return(nil)
		}
		_ = el1.deleteHead(ctx)
		utMgr.segmentNeedBeClean.Store(head.ID.Key(), head)
		time.Sleep(50 * time.Millisecond)

		So(el1.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(el2.size(), ShouldEqual, defaultAppendableSegmentNumber)
		So(util.MapLen(&utMgr.segmentNeedBeClean), ShouldEqual, 0)
		So(util.MapLen(&utMgr.globalSegmentMap), ShouldEqual, defaultAppendableSegmentNumber*2)
		So(util.MapLen(&utMgr.globalBlockMap), ShouldEqual, defaultAppendableSegmentNumber*3*2)
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

func TestEventlogManager_DeleteEventlog(t *testing.T) {
	Convey("test DeleteEventlog", t, func() {
		utMgr := &eventlogManager{segmentReplicaNum: 3}
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		utMgr.volMgr = volMgr
		kvCli := kv.NewMockClient(ctrl)
		utMgr.kvClient = kvCli

		ctx := stdCtx.Background()

		Convey("test deleting", func() {
			// the eventlog doesn't exist
			utMgr.DeleteEventlog(ctx, vanus.NewID())

			md := &metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}
			el, err := newEventlog(ctx, md, kvCli, false)
			So(err, ShouldBeNil)

			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			kvCli.EXPECT().Delete(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			_ = el.add(ctx, createTestSegment(md.ID))
			_ = el.add(ctx, createTestSegment(md.ID))
			_ = el.add(ctx, createTestSegment(md.ID))
			_ = el.add(ctx, createTestSegment(md.ID))

			utMgr.eventLogMap.Store(md.ID.Key(), el)
			utMgr.DeleteEventlog(ctx, md.ID)
			_, exist := mgr.eventLogMap.Load(md.ID.Key())
			So(exist, ShouldBeFalse)
			So(util.MapLen(&utMgr.segmentNeedBeClean), ShouldEqual, 4)
		})

		Convey("make sure delete meta in kv", func() {
			// the eventlog doesn't exist
			utMgr.DeleteEventlog(ctx, vanus.NewID())

			md := &metadata.Eventlog{
				ID:         vanus.NewID(),
				EventbusID: vanus.NewID(),
			}
			el, err := newEventlog(ctx, md, kvCli, false)
			So(err, ShouldBeNil)

			utMgr.eventLogMap.Store(md.ID.Key(), el)
			kvCli.EXPECT().Delete(ctx, metadata.GetEventlogMetadataKey(el.md.ID)).AnyTimes().Return(nil)
			utMgr.DeleteEventlog(ctx, md.ID)
			_, exist := mgr.eventLogMap.Load(md.ID.Key())
			So(exist, ShouldBeFalse)
			So(util.MapLen(&mgr.segmentNeedBeClean), ShouldEqual, 0)
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

		Convey("case: test segment doesn't need to be updated", func() {
			elog, err := newEventlog(ctx, md, kvCli, false)
			So(err, ShouldBeNil)
			seg := createTestSegment(vanus.NewID())
			kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).Times(1).Return(nil)
			err = elog.add(ctx, seg)
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
		})

		Convey("case: test segment to be updated", func() {
			elog, err := newEventlog(ctx, md, kvCli, false)
			So(err, ShouldBeNil)
			seg := createTestSegment(vanus.NewID())
			kvCli.EXPECT().Set(ctx, metadata.GetEventlogSegmentsMetadataKey(elog.md.ID, seg.ID), gomock.Any()).
				Times(1).Return(nil)
			err = elog.add(ctx, seg)
			So(err, ShouldBeNil)
			utMgr.eventLogMap.Store(md.ID.Key(), elog)

			// segment need to be updated
			updateSegment1 := Segment{
				ID:                 seg.ID,
				Size:               1024,
				Number:             16,
				State:              StateWorking,
				FirstEventBornTime: time.Now(),
				LastEventBornTime:  time.Now(),
			}
			segV1 := seg.Copy()
			segV1.Size = updateSegment1.Size
			segV1.Number = updateSegment1.Number
			segV1.State = updateSegment1.State
			segV1.FirstEventBornTime = updateSegment1.FirstEventBornTime
			segV1.LastEventBornTime = updateSegment1.LastEventBornTime
			data, _ := stdJson.Marshal(segV1)
			key := metadata.GetSegmentMetadataKey(seg.ID)
			kvCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {updateSegment1},
			})

			// segment doesn't need to be updated because ID is nil
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {
					{
						LastEventBornTime: time.Now().Add(time.Second),
					},
				},
			})

			// segment need to be updated when last event born time changed
			updateSegment2 := Segment{
				ID:                seg.ID,
				Size:              2048,
				Number:            17,
				LastEventBornTime: time.Now().Add(time.Second),
			}
			segV2 := segV1.Copy()
			segV2.Size = updateSegment2.Size
			segV2.Number = updateSegment2.Number
			segV2.LastEventBornTime = updateSegment2.LastEventBornTime
			data, _ = stdJson.Marshal(segV2)
			kvCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {updateSegment2},
			})

			// mark is full
			updateSegment3 := Segment{
				ID:                seg.ID,
				Size:              4096,
				Number:            18,
				LastEventBornTime: time.Now().Add(time.Second),
				State:             StateFrozen,
			}
			segV3 := segV2.Copy()
			segV3.Size = updateSegment3.Size
			segV3.Number = updateSegment3.Number
			segV3.LastEventBornTime = updateSegment3.LastEventBornTime
			segV3.State = updateSegment3.State
			data, _ = stdJson.Marshal(segV3)
			kvCli.EXPECT().Set(ctx, key, data).Times(1).Return(nil)
			utMgr.UpdateSegment(ctx, map[string][]Segment{
				md.ID.String(): {updateSegment3},
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
		seg := createTestSegment(vanus.NewID())
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

func Test_ExpiredSegmentDeleting(t *testing.T) {
	Convey("test expired segment deleting", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		ctx := stdCtx.Background()
		utMgr := &eventlogManager{
			segmentReplicaNum:           3,
			checkSegmentExpiredInterval: 100 * time.Millisecond,
			segmentExpiredTime:          time.Hour,
		}

		el1, err1 := newEventlog(ctx, &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}, kvCli, false)
		el2, err2 := newEventlog(ctx, &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}, kvCli, false)
		el3, err3 := newEventlog(ctx, &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}, kvCli, false)
		So(err1, ShouldBeNil)
		So(err2, ShouldBeNil)
		So(err3, ShouldBeNil)
		utMgr.eventLogMap.Store(el1.md.ID.Key(), el1)
		utMgr.eventLogMap.Store(el2.md.ID.Key(), el2)
		utMgr.eventLogMap.Store(el3.md.ID.Key(), el3)

		Convey("test clean expired segment", func() {
			kvCli.EXPECT().Delete(gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)

			s11 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-6 * time.Hour),
				LastEventBornTime:  time.Now().Add(-3 * time.Hour),
			}
			el1.segmentList.Set(s11.ID.Uint64(), s11)
			el1.segments = []vanus.ID{s11.ID}

			s21 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-6 * time.Hour),
				LastEventBornTime:  time.Now().Add(-3 * time.Hour),
				State:              StateFrozen,
			}
			s22 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-3 * time.Hour),
				LastEventBornTime:  time.Now().Add(-1 * time.Minute),
			}
			el2.segmentList.Set(s21.ID.Uint64(), s21)
			el2.segmentList.Set(s22.ID.Uint64(), s22)
			el2.segments = []vanus.ID{s21.ID, s22.ID}

			s31 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-6 * time.Hour),
				LastEventBornTime:  time.Now().Add(-3 * time.Hour),
				State:              StateFrozen,
			}
			s32 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-3 * time.Hour),
				LastEventBornTime:  time.Now().Add(-1*time.Hour - time.Minute),
				State:              StateFrozen,
			}
			s33 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-1 * time.Hour),
				LastEventBornTime:  time.Now().Add(-1 * time.Minute),
				State:              StateFrozen,
			}
			s34 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-1 * time.Minute),
				LastEventBornTime:  time.Now().Add(-1 * time.Millisecond),
				State:              StateFrozen,
			}
			el3.segmentList.Set(s31.ID.Uint64(), s31)
			el3.segmentList.Set(s32.ID.Uint64(), s32)
			el3.segmentList.Set(s33.ID.Uint64(), s33)
			el3.segmentList.Set(s34.ID.Uint64(), s34)
			el3.segments = []vanus.ID{s31.ID, s32.ID, s33.ID, s34.ID}
			cCtx, cancel := stdCtx.WithCancel(ctx)
			ch := make(chan struct{})
			go func() {
				utMgr.checkSegmentExpired(cCtx)
				ch <- struct{}{}
			}()
			time.Sleep(time.Second)
			cancel()
			<-ch
			So(el1.segmentList.Len(), ShouldEqual, 1)
			So(el1.segments, ShouldHaveLength, 1)

			So(el2.segmentList.Len(), ShouldEqual, 1)
			So(el2.segments, ShouldHaveLength, 1)
			So(el2.segments[0], ShouldEqual, s22.ID)

			So(el3.segmentList.Len(), ShouldEqual, 2)
			So(el3.segments, ShouldHaveLength, 2)
			So(el3.segments[0], ShouldEqual, s33.ID)
			So(el3.segments[1], ShouldEqual, s34.ID)

			So(util.MapLen(&utMgr.segmentNeedBeClean), ShouldEqual, 3)
		})

		Convey("test kv error", func() {
			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(kv.ErrUnknown)
			kvCli.EXPECT().Delete(gomock.Any(), gomock.Any()).AnyTimes().Return(kv.ErrUnknown)

			s11 := &Segment{
				ID:    vanus.NewID(),
				State: StateFrozen,
			}
			el1.segmentList.Set(s11.ID.Uint64(), s11)
			el1.segments = []vanus.ID{s11.ID}

			s31 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-6 * time.Hour),
				LastEventBornTime:  time.Now().Add(-3 * time.Hour),
				State:              StateFrozen,
			}
			s32 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-3 * time.Hour),
				LastEventBornTime:  time.Now().Add(-1*time.Hour - time.Minute),
				State:              StateFrozen,
			}
			s33 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-1 * time.Hour),
				LastEventBornTime:  time.Now().Add(-1 * time.Minute),
				State:              StateFrozen,
			}
			s34 := &Segment{
				ID:                 vanus.NewID(),
				FirstEventBornTime: time.Now().Add(-1 * time.Minute),
				LastEventBornTime:  time.Now().Add(-1 * time.Millisecond),
				State:              StateFrozen,
			}
			el3.segmentList.Set(s31.ID.Uint64(), s31)
			el3.segmentList.Set(s32.ID.Uint64(), s32)
			el3.segmentList.Set(s33.ID.Uint64(), s33)
			el3.segmentList.Set(s34.ID.Uint64(), s34)
			el3.segments = []vanus.ID{s31.ID, s32.ID, s33.ID, s34.ID}
			cCtx, cancel := stdCtx.WithCancel(ctx)
			ch := make(chan struct{})
			go func() {
				utMgr.checkSegmentExpired(cCtx)
				ch <- struct{}{}
			}()
			time.Sleep(time.Second)
			cancel()
			<-ch

			So(el1.segmentList.Len(), ShouldEqual, 1)
			So(el1.segments, ShouldHaveLength, 1)
			So(el1.head().LastEventBornTime, ShouldEqual, time.Time{})

			So(el3.segmentList.Len(), ShouldEqual, 4)
			So(el3.segments, ShouldHaveLength, 4)
		})

		Convey("test update segment no last event time", func() {
			kvCli.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(nil)

			s11 := &Segment{
				ID:    vanus.NewID(),
				State: StateFrozen,
			}
			el1.segmentList.Set(s11.ID.Uint64(), s11)
			el1.segments = []vanus.ID{s11.ID}

			cCtx, cancel := stdCtx.WithCancel(ctx)
			ch := make(chan struct{})
			go func() {
				utMgr.checkSegmentExpired(cCtx)
				ch <- struct{}{}
			}()
			time.Sleep(time.Second)
			cancel()
			<-ch

			So(el1.segmentList.Len(), ShouldEqual, 1)
			So(el1.segments, ShouldHaveLength, 1)
			minutes := math.Ceil(float64(time.Until(el1.head().LastEventBornTime)) / float64(time.Minute))
			So(minutes, ShouldEqual, 60)
		})
	})
}

func TestEventlog_All(t *testing.T) {
	Convey("test eventlog operation", t, func() {
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		ctx := stdCtx.Background()
		md := &metadata.Eventlog{
			ID:         vanus.NewID(),
			EventbusID: vanus.NewID(),
		}
		el, _ := newEventlog(ctx, md, kvCli, false)

		seg1 := createTestSegment(vanus.NewID())
		seg2 := createTestSegment(vanus.NewID())
		seg3 := createTestSegment(vanus.NewID())
		seg4 := createTestSegment(vanus.NewID())

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

func TestEventlog_MarkSegmentFull(t *testing.T) {
	Convey("test eventlog operation", t, func() {
		ctx := stdCtx.Background()
		ctrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(ctrl)
		md := &metadata.Eventlog{}
		kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		el, _ := newEventlog(ctx, md, kvCli, false)
		seg1 := createTestSegment(md.ID)
		seg2 := createTestSegment(md.ID)
		_ = el.add(ctx, seg1)
		_ = el.add(ctx, seg2)
		seg1.StartOffsetInLog = 111111
		seg1.Number = 12345
		err := el.markSegmentFull(ctx, seg1)
		So(err, ShouldBeNil)
		So(seg2.StartOffsetInLog, ShouldEqual, 111111+12345)
	})
}
