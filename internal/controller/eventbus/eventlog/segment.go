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
	"encoding/json"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	metapb "github.com/linkall-labs/vsproto/pkg/meta"
	"time"
)

type SegmentState string

const (
	StateCreated  = SegmentState("created")
	StateWorking  = SegmentState("working")
	StateFrozen   = SegmentState("frozen")
	StateArchived = SegmentState("archived")
	StateExpired  = SegmentState("expired")
)

type Segment struct {
	ID                vanus.ID      `json:"id,omitempty"`
	Capacity          int64         `json:"capacity,omitempty"`
	EventLogID        vanus.ID      `json:"event_log_id,omitempty"`
	PreviousSegmentId vanus.ID      `json:"previous_segment_id,omitempty"`
	NextSegmentId     vanus.ID      `json:"next_segment_id,omitempty"`
	StartOffsetInLog  int64         `json:"start_offset_in_log,omitempty"`
	Replicas          *ReplicaGroup `json:"replicas,omitempty"`
	State             SegmentState  `json:"state,omitempty"`
	Size              int64         `json:"size,omitempty"`
	Number            int32         `json:"number,omitempty"`
}

func (seg *Segment) IsAppendable() bool {
	return seg.isReady() && seg.State == StateWorking
}

func (seg *Segment) GetLeaderBlock() *metadata.Block {
	if !seg.isReady() {
		return nil
	}
	return seg.Replicas.Peers[seg.Replicas.Leader]
}

func (seg *Segment) String() string {
	data, _ := json.Marshal(seg)
	return string(data)
}

// TODO Don't update field in here
func (seg *Segment) isNeedUpdate(newSeg Segment) bool {
	if seg.ID != newSeg.ID {
		return false
	}
	needed := false
	if seg.Size != newSeg.Size {
		seg.Size = newSeg.Size
		needed = true
	}
	if seg.Number != newSeg.Number {
		seg.Number = newSeg.Number
		needed = true
	}
	if newSeg.State != "" && seg.State != newSeg.State {
		seg.State = newSeg.State
		needed = true
	}
	return needed
}

func (seg *Segment) isReady() bool {
	return seg.Replicas != nil && seg.Replicas.Leader > 0
}

func (seg *Segment) changeLeaderAddr(id int) {
	seg.Replicas.Leader = id
}

type ReplicaGroup struct {
	ID        vanus.ID                `json:"id"`
	Leader    int                     `json:"leader"`
	Peers     map[int]*metadata.Block `json:"blocks"`
	CreateAt  time.Time               `json:"create_at"`
	DestroyAt time.Time               `json:"destroy_at"`
}

func Convert2ProtoSegment(ins ...*Segment) []*metapb.Segment {
	segs := make([]*metapb.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		blocks := map[uint64]*metapb.Block{}
		topo := mgr.getSegmentTopology(seg)
		for _, v := range seg.Replicas.Peers {
			blocks[v.ID.Uint64()] = &metapb.Block{
				Id:       v.ID.Uint64(),
				Endpoint: topo[v.ID.Uint64()],
				VolumeID: v.VolumeID.Uint64(),
			}
		}
		segs[idx] = &metapb.Segment{
			Id:                seg.ID.Uint64(),
			PreviousSegmentId: seg.PreviousSegmentId.Uint64(),
			NextSegmentId:     seg.NextSegmentId.Uint64(),
			EventLogId:        seg.EventLogID.Uint64(),
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Replicas:          blocks,
			State:             string(seg.State),
			LeaderBlockId:     seg.GetLeaderBlock().ID.Uint64(),
		}

		if segs[idx].NumberEventStored == 0 {
			segs[idx].EndOffsetInLog = -1
		}
	}
	return segs
}
