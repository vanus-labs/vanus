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

package acclocator

import (
	"context"
	"encoding/json"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
	"strings"
	"sync"
)

const (
	defaultSegmentBlockSize        = 64 * 1024 * 1024
	segmentBlockKeyPrefixInKVStore = "/vanus/internal/resource/segmentBlock"
)

type LogSegment struct {
	ID                string   `json:"id"`
	Capacity          int64    `json:"capacity"`
	Size              int64    `json:"size"`
	VolumeID          string   `json:"volume_id"`
	EventLogID        string   `json:"event_log_id"`
	ReplicaGroupID    string   `json:"replica_group_id"`
	PeersAddress      []string `json:"peers_address"`
	Number            int32    `json:"number"`
	PreviousSegmentId string   `json:"previous_segment_id"`
	NextSegmentId     string   `json:"next_segment_id"`
	StartOffsetInLog  int64    `json:"start_offset_in_log"`
	IsFull            bool     `json:"is_full"`
}

type Allocator interface {
	Init(ctx context.Context, kvCli kv.Client) error
	Pick(ctx context.Context, num int, size int64) ([]*LogSegment, error)
	Remove(ctx context.Context, seg *LogSegment) error
}

func NewAllocator(volMgr volume.Manager, selector VolumeSelector) Allocator {
	return &allocator{
		selectorForSegmentCreate: selector,
		volumeMgr:                volMgr,
	}
}

type allocator struct {
	selectorForSegmentCreate VolumeSelector
	eventLogSegment          map[string]*skiplist.SkipList
	segmentMap               sync.Map
	kvClient                 kv.Client
	volumeMgr                volume.Manager
}

func (mgr *allocator) Init(ctx context.Context, kvCli kv.Client) error {
	mgr.kvClient = kvCli
	go mgr.dynamicAllocateSegmentTask()
	mgr.selectorForSegmentCreate = NewVolumeRoundRobin(mgr.volumeMgr.GetAllVolume)
	pairs, err := mgr.kvClient.List(ctx, segmentBlockKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	// TODO unassigned -> assigned
	for idx := range pairs {
		pair := pairs[idx]
		sbInfo := &volume.SegmentBlock{}
		err := json.Unmarshal(pair.Value, sbInfo)
		if err != nil {
			return err
		}
		l, exist := mgr.eventLogSegment[sbInfo.EventLogID]
		if !exist {
			l = skiplist.New(skiplist.String)
			mgr.eventLogSegment[sbInfo.EventLogID] = l
		}
		l.Set(sbInfo.ID, sbInfo)
		mgr.segmentMap.Store(sbInfo.ID, sbInfo)
	}
	return nil
}

func (mgr *allocator) Pick(ctx context.Context, num int, size int64) ([]*LogSegment, error) {
	segArr := make([]*LogSegment, num+1)
	var err error
	defer func() {
		if err == nil {
			return
		}
		for idx := 0; idx < num; idx++ {
			if idx == 0 && segArr[idx] != nil {
				segArr[idx].NextSegmentId = ""
			} else {
				segArr[idx].EventLogID = ""
				segArr[idx].ReplicaGroupID = ""
				segArr[idx].PeersAddress = nil
			}
		}
		for idx := 0; idx < num; idx++ {
			if segArr[idx] != nil {
				if _err := mgr.updateSegmentBlockInKV(ctx, segArr[idx]); _err != nil {
					log.Error(ctx, "update acclocator info in kv store failed when cancel binding", map[string]interface{}{
						log.KeyError: _err,
					})
				}
			}
		}
	}()

	//segArr[0] = mgr.getLastSegmentOfEventLog(el)
	//for idx := 1; idx < len(segArr); idx++ {
	//	seg, err := mgr.pickSegment(ctx, defaultSegmentBlockSize)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	// binding, assign runtime fields
	//	seg.EventLogID = el.ID
	//	// TODO use replica structure to replace here
	//	if err = mgr.createSegmentBlockReplicaGroup(seg); err != nil {
	//		return nil, err
	//	}
	//	if err = mgr.volumeMgr.GetVolumeByID(seg.VolumeID).ActivateSegment(ctx, seg); err != nil {
	//		return nil, err
	//	}
	//	if segArr[idx-1] != nil {
	//		segArr[idx-1].NextSegmentId = seg.ID
	//		seg.PreviousSegmentId = segArr[idx-1].ID
	//	}
	//	segArr[idx] = seg
	//}
	//
	//sl, exist := mgr.eventLogSegment[el.ID]
	//if !exist {
	//	sl = skiplist.New(skiplist.String)
	//	mgr.eventLogSegment[el.ID] = sl
	//}
	//for idx := range segArr {
	//	if segArr[idx] == nil {
	//		continue
	//	}
	//	sl.Set(segArr[idx].ID, segArr[idx])
	//	if err = mgr.updateSegmentBlockInKV(ctx, segArr[idx]); err != nil {
	//		return nil, err
	//	}
	//}
	return segArr, nil
}

func (mgr *allocator) Remove(ctx context.Context, seg *LogSegment) error {
	return nil
}

func (mgr *allocator) pickSegment(ctx context.Context, size int64) (*volume.SegmentBlock, error) {
	// no enough acclocator, manually allocate and bind
	return mgr.allocateSegmentImmediately(ctx, defaultSegmentBlockSize)
}

func (mgr *allocator) destroy() error {
	return nil
}

func (mgr *allocator) allocateSegmentImmediately(ctx context.Context, size int64) (*volume.SegmentBlock, error) {
	volIns := mgr.selectorForSegmentCreate.Select(ctx, size)
	if volIns == nil {
		return nil, errors.New("no volume available")
	}
	segmentInfo, err := volIns.CreateSegment(ctx, size)
	if err != nil {
		return nil, err
	}
	return segmentInfo, nil
}

func (mgr *allocator) dynamicAllocateSegmentTask() {
	//TODO
}

func (mgr *allocator) createSegmentBlockReplicaGroup(segInfo *volume.SegmentBlock) error {
	// TODO implement
	segInfo.ReplicaGroupID = "group-1"
	segInfo.PeersAddress = []string{"ip1", "ip2"}
	// pick 2 segments with same capacity
	return nil
}

func (mgr *allocator) getSegmentBlockByID(ctx context.Context, id string) *volume.SegmentBlock {
	v, exist := mgr.segmentMap.Load(id)
	if !exist {
		return nil
	}

	return v.(*volume.SegmentBlock)
}

func (mgr *allocator) getSegmentBlockKeyInKVStore(sbID string) string {
	return strings.Join([]string{segmentBlockKeyPrefixInKVStore, sbID}, "/")
}

func (mgr *allocator) updateSegmentBlockInKV(ctx context.Context, segment *LogSegment) error {
	if segment == nil {
		return nil
	}
	data, err := json.Marshal(segment)
	if err != nil {
		return err
	}
	return mgr.kvClient.Set(ctx, mgr.getSegmentBlockKeyInKVStore(segment.ID), data)
}

func (mgr *allocator) getLastSegmentOfEventLog(el *info.EventLogInfo) *volume.SegmentBlock {
	sl := mgr.eventLogSegment[el.ID]
	if sl == nil {
		return nil
	}
	val := sl.Back().Value
	if val == nil {
		return nil
	}
	return val.(*volume.SegmentBlock)
}
