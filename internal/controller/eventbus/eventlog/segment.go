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
	"context"
	"encoding/json"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
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
	ID                 vanus.ID      `json:"id,omitempty"`
	Capacity           int64         `json:"capacity,omitempty"`
	EventLogID         vanus.ID      `json:"event_log_id,omitempty"`
	PreviousSegmentID  vanus.ID      `json:"previous_segment_id,omitempty"`
	NextSegmentID      vanus.ID      `json:"next_segment_id,omitempty"`
	StartOffsetInLog   int64         `json:"start_offset_in_log,omitempty"`
	Replicas           *ReplicaGroup `json:"replicas,omitempty"`
	State              SegmentState  `json:"state,omitempty"`
	Size               int64         `json:"size,omitempty"`
	Number             int32         `json:"number,omitempty"`
	FirstEventBornTime time.Time     `json:"first_event_born_time"`
	LastEventBornTime  time.Time     `json:"last_event_born_time"`
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
	if seg.isFull() {
		return false
	}
	if seg.ID != newSeg.ID {
		return false
	}
	needed := false
	if seg.Size < newSeg.Size {
		seg.Size = newSeg.Size
		needed = true
	}
	if seg.Number < newSeg.Number {
		seg.Number = newSeg.Number
		needed = true
	}
	// TODO(wenfeng): follow state shift
	if newSeg.State != "" && seg.State != newSeg.State {
		seg.State = newSeg.State
		needed = true
	}

	if newSeg.FirstEventBornTime.After(seg.FirstEventBornTime) {
		seg.FirstEventBornTime = newSeg.FirstEventBornTime
		needed = true
	}
	if newSeg.LastEventBornTime.After(seg.LastEventBornTime) {
		seg.LastEventBornTime = newSeg.LastEventBornTime
		needed = true
	}
	return needed
}

func (seg *Segment) isFull() bool {
	return seg.State == StateFrozen
}

func (seg *Segment) isReady() bool {
	return seg.Replicas != nil && seg.Replicas.Leader > 0
}

func (seg *Segment) Copy() Segment {
	return Segment{
		ID:                 seg.ID,
		Capacity:           seg.Capacity,
		EventLogID:         seg.EventLogID,
		PreviousSegmentID:  seg.PreviousSegmentID,
		NextSegmentID:      seg.NextSegmentID,
		StartOffsetInLog:   seg.StartOffsetInLog,
		Replicas:           seg.Replicas,
		State:              seg.State,
		Size:               seg.Size,
		Number:             seg.Number,
		FirstEventBornTime: seg.FirstEventBornTime,
		LastEventBornTime:  seg.LastEventBornTime,
	}
}

type ReplicaGroup struct {
	ID        vanus.ID                   `json:"id"`
	Leader    uint64                     `json:"leader"`
	Peers     map[uint64]*metadata.Block `json:"blocks"`
	Term      uint64                     `json:"term"`
	CreateAt  time.Time                  `json:"create_at"`
	DestroyAt time.Time                  `json:"destroy_at"`
}

func Convert2ProtoSegment(ctx context.Context, ins ...*Segment) []*metapb.Segment {
	segs := make([]*metapb.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		blocks := map[uint64]*metapb.Block{}
		if seg.isReady() {
			topo := mgr.getSegmentTopology(ctx, seg)
			for _, v := range seg.Replicas.Peers {
				blocks[v.ID.Uint64()] = &metapb.Block{
					Id:       v.ID.Uint64(),
					Endpoint: topo[v.ID.Uint64()],
					VolumeID: v.VolumeID.Uint64(),
				}
			}
		}
		segs[idx] = &metapb.Segment{
			Id:                seg.ID.Uint64(),
			PreviousSegmentId: seg.PreviousSegmentID.Uint64(),
			NextSegmentId:     seg.NextSegmentID.Uint64(),
			EventLogId:        seg.EventLogID.Uint64(),
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Replicas:          blocks,
			State:             string(seg.State),
		}
		if seg.GetLeaderBlock() != nil {
			segs[idx].LeaderBlockId = seg.GetLeaderBlock().ID.Uint64()
		}

		if segs[idx].NumberEventStored == 0 {
			segs[idx].EndOffsetInLog = -1
		}
	}
	return segs
}
