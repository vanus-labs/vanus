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
	ID        string
	Namespace string
	Name      string
	LogNumber int
	EventLogs []*EventLogInfo
	VRN       *meta.VanusResourceName
}

func Convert2ProtoEventBus(ins ...*BusInfo) []*meta.EventBus {
	pebs := make([]*meta.EventBus, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eb := ins[idx]
		pebs[idx] = &meta.EventBus{
			Namespace: eb.Namespace,
			Name:      eb.Name,
			LogNumber: int32(eb.LogNumber),
			Logs:      Convert2ProtoEventLog(eb.EventLogs...),
			Vrn:       eb.VRN,
		}
	}
	return pebs
}

type EventLogInfo struct {
	// global unique id
	ID                    int64
	EventBusVRN           *meta.VanusResourceName
	CurrentSegmentNumbers int
	VRN                   *meta.VanusResourceName
	SegmentList           []*SegmentBlockInfo
}

func Convert2ProtoEventLog(ins ...*EventLogInfo) []*meta.EventLog {
	pels := make([]*meta.EventLog, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		eli := ins[idx]
		pels[idx] = &meta.EventLog{
			EventLogId:            eli.ID,
			BusVrn:                eli.EventBusVRN,
			CurrentSegmentNumbers: int32(eli.CurrentSegmentNumbers),
			Vrn:                   eli.VRN,
		}
	}
	return pels
}

type SegmentBlockInfo struct {
	ID             string
	Capacity       int64
	Size           int64
	VolumeInfo     *VolumeInfo
	EventLogID     string
	ReplicaGroupID string
	PeersAddress   []string
}

func Convert2ProtoSegment(ins ...*SegmentBlockInfo) []*meta.Segment {
	segs := make([]*meta.Segment, len(ins))
	for idx := 0; idx < len(ins); idx++ {
		seg := ins[idx]
		segs[idx] = &meta.Segment{
			Id: seg.ID,
			// TODO RENAME
			StorageUri: seg.VolumeInfo.AssignedSegmentServer.Address,
			Size:       seg.Size,
			Capacity:   seg.Capacity,
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
	Capacity                 int64
	Used                     int64
	BlockNumbers             int
	Blocks                   map[string]string
	PersistenceVolumeClaimID string
	AssignedSegmentServer    *SegmentServerInfo
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
