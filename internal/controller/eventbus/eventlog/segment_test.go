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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestConvert2ProtoSegment(t *testing.T) {
	Convey("test convert Segment to protobuf structure", t, func() {
		ctrl := gomock.NewController(t)
		volMgr := volume.NewMockManager(ctrl)
		mgr.volMgr = volMgr

		segID := vanus.NewID()
		eID := vanus.NewID()
		block1 := vanus.NewID()
		block2 := vanus.NewID()
		block3 := vanus.NewID()
		seg := &Segment{
			ID:                segID,
			Capacity:          64 * 1024 * 1024,
			EventLogID:        eID,
			PreviousSegmentID: vanus.NewID(),
			NextSegmentID:     vanus.NewID(),
			StartOffsetInLog:  1000,
			Replicas: &ReplicaGroup{
				ID:     vanus.NewID(),
				Leader: block1.Uint64(),
				Peers: map[uint64]*metadata.Block{
					block1.Uint64(): {
						ID:         block1,
						Capacity:   64 * 1024 * 1024,
						Size:       1234,
						VolumeID:   vanus.NewID(),
						EventlogID: eID,
						SegmentID:  segID,
					},
					block2.Uint64(): {
						ID:         block2,
						Capacity:   64 * 1024 * 1024,
						Size:       1234,
						VolumeID:   vanus.NewID(),
						EventlogID: eID,
						SegmentID:  segID,
					},
					block3.Uint64(): {
						ID:         block3,
						Capacity:   64 * 1024 * 1024,
						Size:       1234,
						VolumeID:   vanus.NewID(),
						EventlogID: eID,
						SegmentID:  segID,
					},
				},
				Term:     1,
				CreateAt: time.Now(),
			},
			State:  StateWorking,
			Size:   1234,
			Number: 3,
		}

		ins1 := server.NewMockInstance(ctrl)
		ins2 := server.NewMockInstance(ctrl)
		ins3 := server.NewMockInstance(ctrl)
		volMgr.EXPECT().GetVolumeInstanceByID(gomock.Any()).Times(9).DoAndReturn(func(id vanus.ID) server.Instance {
			switch id {
			case seg.Replicas.Peers[block1.Uint64()].VolumeID:
				return ins1
			case seg.Replicas.Peers[block2.Uint64()].VolumeID:
				return ins2
			case seg.Replicas.Peers[block3.Uint64()].VolumeID:
				return ins3
			}
			return nil
		})
		ins1.EXPECT().Address().Times(3).Return("127.0.0.1:10001")
		ins2.EXPECT().Address().Times(3).Return("127.0.0.1:10002")
		ins3.EXPECT().Address().Times(3).Return("")

		pbSegs := Convert2ProtoSegment(stdCtx.Background(), seg, seg, seg)
		So(pbSegs, ShouldHaveLength, 3)
		So(pbSegs[0], ShouldResemble, pbSegs[1])
		So(pbSegs[1], ShouldResemble, pbSegs[2])
		So(pbSegs[0].Id, ShouldEqual, seg.ID.Uint64())
		So(pbSegs[0].PreviousSegmentId, ShouldEqual, seg.PreviousSegmentID.Uint64())
		So(pbSegs[0].NextSegmentId, ShouldEqual, seg.NextSegmentID.Uint64())
		So(pbSegs[0].EventLogId, ShouldEqual, eID.Uint64())
		So(pbSegs[0].StartOffsetInLog, ShouldEqual, seg.StartOffsetInLog)
		So(pbSegs[0].EndOffsetInLog, ShouldEqual, seg.StartOffsetInLog+int64(seg.Number)-1)
		So(pbSegs[0].Size, ShouldEqual, seg.Size)
		So(pbSegs[0].Capacity, ShouldEqual, seg.Capacity)
		So(pbSegs[0].NumberEventStored, ShouldEqual, seg.Number)
		So(pbSegs[0].State, ShouldEqual, seg.State)
		So(pbSegs[0].LeaderBlockId, ShouldEqual, seg.Replicas.Leader)
		So(pbSegs[0].Replicas, ShouldHaveLength, 3)
		So(pbSegs[0].Replicas[block1.Uint64()], ShouldNotBeNil)
		So(pbSegs[0].Replicas[block2.Uint64()], ShouldNotBeNil)
		So(pbSegs[0].Replicas[block3.Uint64()], ShouldNotBeNil)

		So(pbSegs[0].Replicas[block1.Uint64()].Id, ShouldEqual, seg.Replicas.Peers[block1.Uint64()].ID.Uint64())
		So(pbSegs[0].Replicas[block1.Uint64()].VolumeID, ShouldEqual, seg.Replicas.Peers[block1.Uint64()].VolumeID.Uint64())
		So(pbSegs[0].Replicas[block1.Uint64()].Endpoint, ShouldEqual, "127.0.0.1:10001")
		So(pbSegs[0].Replicas[block3.Uint64()].Id, ShouldEqual, seg.Replicas.Peers[block3.Uint64()].ID.Uint64())
		So(pbSegs[0].Replicas[block3.Uint64()].VolumeID, ShouldEqual, seg.Replicas.Peers[block3.Uint64()].VolumeID.Uint64())
		So(pbSegs[0].Replicas[block3.Uint64()].Endpoint, ShouldEqual, "")
	})
}

func TestSegment_Copy(t *testing.T) {
	Convey("test segment copy", t, func() {
		seg := createTestSegment(vanus.NewID())
		seg.Capacity = 12345678
		seg.EventLogID = vanus.NewID()
		seg.PreviousSegmentID = vanus.NewID()
		seg.NextSegmentID = vanus.NewID()
		seg.StartOffsetInLog = 12345
		seg.State = StateCreated
		seg.Size = 1234567
		seg.Number = 123456
		seg.FirstEventBornTime = time.Now()
		seg.LastEventBornTime = time.Now()

		segV1 := seg.Copy()
		So(segV1.Capacity, ShouldEqual, seg.Capacity)
		So(segV1.EventLogID, ShouldEqual, seg.EventLogID)
		So(segV1.PreviousSegmentID, ShouldEqual, seg.PreviousSegmentID)
		So(segV1.NextSegmentID, ShouldEqual, seg.NextSegmentID)
		So(segV1.StartOffsetInLog, ShouldEqual, seg.StartOffsetInLog)
		So(segV1.State, ShouldEqual, seg.State)
		So(segV1.Size, ShouldEqual, seg.Size)
		So(segV1.Number, ShouldEqual, seg.Number)
		So(segV1.FirstEventBornTime, ShouldEqual, seg.FirstEventBornTime)
		So(segV1.LastEventBornTime, ShouldEqual, seg.LastEventBornTime)

		segV1.Size = 7654321
		segV1.Number = 654321
		So(seg.Size, ShouldEqual, 1234567)
		So(seg.Number, ShouldEqual, 123456)

		segV1.Replicas.Term = 1
		So(seg.Replicas.Term, ShouldEqual, 1)
	})
}

func createTestSegment(volID vanus.ID) *Segment {
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
					ID:       leader,
					Capacity: 64 * 1024 * 1024,
					VolumeID: volID,
				},
				fo1.Uint64(): {
					ID:       fo1,
					Capacity: 64 * 1024 * 1024,
					VolumeID: volID,
				},
				fo2.Uint64(): {
					ID:       fo2,
					Capacity: 64 * 1024 * 1024,
					VolumeID: volID,
				},
			},
			Term:     0,
			CreateAt: time.Now(),
		},
	}
}
