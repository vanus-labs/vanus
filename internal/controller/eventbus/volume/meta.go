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

package volume

import (
	"github.com/linkall-labs/vanus/internal/controller/eventbus/block"
	"github.com/linkall-labs/vsproto/pkg/meta"
	"time"
)

type Metadata struct {
	ID           uint64            `json:"id"`
	Capacity     int64             `json:"capacity"`
	Used         int64             `json:"used"`
	BlockNumbers int               `json:"block_numbers"`
	Blocks       map[string]string `json:"blocks"`
}

type SegmentState string

const (
	StateCreated  = SegmentState("created")
	StateWorking  = SegmentState("working")
	StateFrozen   = SegmentState("frozen")
	StateArchived = SegmentState("archived")
	StateExpired  = SegmentState("expired")
)

type Segment struct {
	ID                uint64        `json:"id"`
	State             SegmentState  `json:"state"`
	Capacity          int64         `json:"capacity"`
	Size              int64         `json:"size"`
	VolumeID          uint64        `json:"volume_id"`
	EventLogID        uint64        `json:"event_log_id"`
	Number            int32         `json:"number"`
	PreviousSegmentId uint64        `json:"previous_segment_id"`
	NextSegmentId     uint64        `json:"next_segment_id"`
	StartOffsetInLog  int64         `json:"start_offset_in_log"`
	Replicas          *ReplicaGroup `json:"replicas"`
}

func (seg *Segment) IsAppendable() bool {
	return seg.isReady() && seg.State == StateWorking
}

func (seg *Segment) GetLeaderAddress() string {
	if !seg.isReady() {
		return ""
	}
	return mgr.GetVolumeInstanceByID(seg.Replicas.LeaderID).Address()
}

func (seg *Segment) isReady() bool {
	return seg.Replicas != nil && seg.Replicas.LeaderID > 0
}

type ReplicaGroup struct {
	ID           uint64                  `json:"id"`
	PeersAddress []string                `json:"peers_address"`
	LeaderID     uint64                  `json:"leader_id"`
	Blocks       map[uint64]*block.Block `json:"blocks"`
	CreateAt     time.Time               `json:"create_at"`
	DestroyAt    time.Time               `json:"destroy_at"`
}

func (rg *ReplicaGroup) Peers() []string {
	peers := make([]string, 0)
	for _, v := range rg.Blocks {
		ins := mgr.GetVolumeInstanceByID(v.VolumeID)
		if ins == nil {
			continue
		}
		peers = append(peers, ins.Address())
	}
	return peers
}

func Convert2ProtoSegment(ins ...*Segment) []*meta.Segment {
	segs := make([]*meta.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		// TODO optimize reported info
		segs[idx] = &meta.Segment{
			Id:                seg.ID,
			PreviousSegmentId: seg.PreviousSegmentId,
			NextSegmentId:     seg.NextSegmentId,
			EventLogId:        seg.EventLogID,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			State:             string(seg.State),
			ReplicaId:         seg.ID,
			LeaderAddr:        seg.GetLeaderAddress(),
		}
		if segs[idx].NumberEventStored == 0 {
			segs[idx].EndOffsetInLog = -1
		}
	}
	return segs
}
