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

package info

import (
	"crypto/sha256"
	"fmt"
	"github.com/linkall-labs/vsproto/pkg/meta"
)

type BusInfo struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	LogNumber int             `json:"log_number"`
	EventLogs []*EventLogInfo `json:"event_logs"`
}

func Convert2ProtoEventBus(ins ...*BusInfo) []*meta.EventBus {
	pebs := make([]*meta.EventBus, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eb := ins[idx]
		pebs[idx] = &meta.EventBus{
			Name:      eb.Name,
			LogNumber: int32(eb.LogNumber),
			Logs:      Convert2ProtoEventLog(eb.EventLogs...),
		}
	}
	return pebs
}

type EventLogInfo struct {
	// global unique id
	ID                    string   `json:"id"`
	EventBusName          string   `json:"eventbus_name"`
	CurrentSegmentNumbers int      `json:"current_segment_numbers"`
	SegmentList           []string `json:"segment_list"`
}

func Convert2ProtoEventLog(ins ...*EventLogInfo) []*meta.EventLog {
	pels := make([]*meta.EventLog, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eli := ins[idx]
		pels[idx] = &meta.EventLog{
			EventBusName:          eli.EventBusName,
			EventLogId:            eli.ID,
			CurrentSegmentNumbers: int32(eli.CurrentSegmentNumbers),
			ServerAddress:         "127.0.0.1:2048",
		}
	}
	return pels
}

type SegmentBlockInfo struct {
	ID                string      `json:"id"`
	Capacity          int64       `json:"capacity"`
	Size              int64       `json:"size"`
	VolumeInfo        *VolumeInfo `json:"volume_info"`
	EventLogID        string      `json:"event_log_id"`
	ReplicaGroupID    string      `json:"replica_group_id"`
	PeersAddress      []string    `json:"peers_address"`
	Number            int32       `json:"number"`
	PreviousSegmentId string      `json:"previous_segment_id"`
	NextSegmentId     string      `json:"next_segment_id"`
	StartOffsetInLog  int64       `json:"start_offset_in_log"`
	IsFull            bool        `json:"is_full"`
}

func Convert2ProtoSegment(ins ...*SegmentBlockInfo) []*meta.Segment {
	segs := make([]*meta.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		// TODO optimize reported info
		segs[idx] = &meta.Segment{
			Id:                seg.ID,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number),
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
		}
		if seg.VolumeInfo != nil && seg.VolumeInfo.AssignedSegmentServer != nil {
			segs[idx].StorageUri = seg.VolumeInfo.AssignedSegmentServer.Address
		}

	}
	return segs
}

type SegmentServerInfo struct {
	id      string
	Address string
	Volume  *VolumeInfo
}

func (in *SegmentServerInfo) ID() string {
	if in.id == "" {
		in.id = fmt.Sprintf("%x", sha256.Sum256([]byte(in.Address)))
	}
	return in.id
}

type VolumeInfo struct {
	Capacity                 int64              `json:"capacity"`
	Used                     int64              `json:"used"`
	BlockNumbers             int                `json:"block_numbers"`
	Blocks                   map[string]string  `json:"blocks"`
	PersistenceVolumeClaimID string             `json:"persistence_volume_claim_id"`
	AssignedSegmentServer    *SegmentServerInfo `json:"-"`
}

func (in *VolumeInfo) ID() string {
	return in.PersistenceVolumeClaimID
}

func (in *VolumeInfo) AddBlock(bi *SegmentBlockInfo) {
	in.Used += bi.Capacity
	in.Blocks[bi.ID] = bi.EventLogID
}

func (in *VolumeInfo) RemoveBlock(bi *SegmentBlockInfo) {
	in.Used -= bi.Capacity
	delete(in.Blocks, bi.ID)
}
