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
	meta "github.com/linkall-labs/vsproto/pkg/meta"
	"time"
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
	ID           string `json:"id"`
	EventBusName string `json:"eventbus_name"`
}

func Convert2ProtoEventLog(ins ...*EventLogInfo) []*meta.EventLog {
	pels := make([]*meta.EventLog, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eli := ins[idx]
		pels[idx] = &meta.EventLog{
			EventBusName:  eli.EventBusName,
			EventLogId:    eli.ID,
			ServerAddress: "127.0.0.1:2048",
		}
	}
	return pels
}

type SegmentBlockInfo struct {
	ID                string      `json:"id"`
	Capacity          int64       `json:"capacity"`
	Size              int64       `json:"size"`
	VolumeID          string      `json:"volume_id"`
	VolumeInfo        *VolumeInfo `json:"-"`
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
			PreviousSegmentId: seg.PreviousSegmentId,
			NextSegmentId:     seg.NextSegmentId,
			EventLogId:        seg.EventLogID,
			StartOffsetInLog:  seg.StartOffsetInLog,
			EndOffsetInLog:    seg.StartOffsetInLog + int64(seg.Number) - 1,
			Size:              seg.Size,
			Capacity:          seg.Capacity,
			NumberEventStored: seg.Number,
			Tier:              meta.StorageTier_SSD,
		}
		if segs[idx].NumberEventStored == 0 {
			segs[idx].EndOffsetInLog = -1
		}
		if seg.VolumeInfo != nil && seg.VolumeInfo.assignedSegmentServer != nil {
			segs[idx].StorageUri = seg.VolumeInfo.assignedSegmentServer.Address
		}

	}
	return segs
}

type SegmentServerInfo struct {
	id                string
	Address           string
	Volume            *VolumeInfo
	StartedAt         time.Time
	LastHeartbeatTime time.Time
}

func (in *SegmentServerInfo) ID() string {
	if in.id == "" {
		in.id = fmt.Sprintf("%x", sha256.Sum256([]byte(in.Address)))
	}
	return in.id
}

const (
	volumeAliveInterval = 5 * time.Second
)

type VolumeInfo struct {
	Capacity                 int64              `json:"capacity"`
	Used                     int64              `json:"used"`
	BlockNumbers             int                `json:"block_numbers"`
	Blocks                   map[string]string  `json:"blocks"`
	PersistenceVolumeClaimID string             `json:"persistence_volume_claim_id"`
	assignedSegmentServer    *SegmentServerInfo `json:"-"`
	isActive                 bool
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

func (in *VolumeInfo) IsOnline() bool {
	return in.assignedSegmentServer != nil && time.Now().Sub(in.assignedSegmentServer.LastHeartbeatTime) < volumeAliveInterval
}

func (in *VolumeInfo) IsActivity() bool {
	return in.isActive
}

func (in *VolumeInfo) Activate(serverInfo *SegmentServerInfo) {
	in.isActive = true
	in.assignedSegmentServer = serverInfo
	serverInfo.Volume = in
}

func (in *VolumeInfo) Inactivate() {
	in.isActive = false
	in.assignedSegmentServer = nil
}

func (in *VolumeInfo) GetAccessEndpoint() string {
	if !in.isActive {
		return ""
	}
	return in.assignedSegmentServer.Address
}
