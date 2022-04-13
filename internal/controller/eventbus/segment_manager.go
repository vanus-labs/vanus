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

package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/selector"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/observability/log"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"
)

const (
	defaultSegmentBlockSize = 64 * 1024 * 1024
)

var (
	ErrEventLogNotFound = errors.New("eventlog not found")
)

type segmentManager struct {
	selectorForSegmentCreate selector.VolumeSelector
	eventLogSegment          map[string]*skiplist.SkipList
	segmentMap               sync.Map
	kvStore                  kv.Client
	volumeMgr                volume.Manager
}

func newSegmentMgr(ctrl *controller) *segmentManager {
	return &segmentManager{
		eventLogSegment: map[string]*skiplist.SkipList{},
		kvStore:         ctrl.kvStore,
		volumeMgr:       ctrl.volumeMgr,
	}
}

func (pool *segmentManager) init(ctx context.Context) error {
	go pool.dynamicAllocateSegmentTask()
	pool.selectorForSegmentCreate = selector.NewVolumeRoundRobin(pool.volumeMgr.GetAllVolume)
	pairs, err := pool.kvStore.List(ctx, segmentBlockKeyPrefixInKVStore)
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
		l, exist := pool.eventLogSegment[sbInfo.EventLogID]
		if !exist {
			l = skiplist.New(skiplist.String)
			pool.eventLogSegment[sbInfo.EventLogID] = l
		}
		l.Set(sbInfo.ID, sbInfo)
		pool.segmentMap.Store(sbInfo.ID, sbInfo)
	}
	return nil
}

func (pool *segmentManager) destroy() error {
	return nil
}

func (pool *segmentManager) bindSegment(ctx context.Context, el *info.EventLogInfo, num int) ([]*volume.SegmentBlock, error) {
	segArr := make([]*volume.SegmentBlock, num+1)
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
				if _err := pool.updateSegmentBlockInKV(ctx, segArr[idx]); _err != nil {
					log.Error(ctx, "update segment info in kv store failed when cancel binding", map[string]interface{}{
						log.KeyError: _err,
					})
				}
			}
		}
	}()

	segArr[0] = pool.getLastSegmentOfEventLog(el)
	for idx := 1; idx < len(segArr); idx++ {
		seg, err := pool.pickSegment(ctx, defaultSegmentBlockSize)
		if err != nil {
			return nil, err
		}

		// binding, assign runtime fields
		seg.EventLogID = el.ID
		// TODO use replica structure to replace here
		if err = pool.createSegmentBlockReplicaGroup(seg); err != nil {
			return nil, err
		}
		if err = pool.volumeMgr.GetVolumeByID(seg.VolumeID).ActivateSegment(ctx, seg); err != nil {
			return nil, err
		}
		if segArr[idx-1] != nil {
			segArr[idx-1].NextSegmentId = seg.ID
			seg.PreviousSegmentId = segArr[idx-1].ID
		}
		segArr[idx] = seg
	}

	sl, exist := pool.eventLogSegment[el.ID]
	if !exist {
		sl = skiplist.New(skiplist.String)
		pool.eventLogSegment[el.ID] = sl
	}
	for idx := range segArr {
		if segArr[idx] == nil {
			continue
		}
		sl.Set(segArr[idx].ID, segArr[idx])
		if err = pool.updateSegmentBlockInKV(ctx, segArr[idx]); err != nil {
			return nil, err
		}
	}
	return segArr, nil
}

func (pool *segmentManager) pickSegment(ctx context.Context, size int64) (*volume.SegmentBlock, error) {
	// no enough segment, manually allocate and bind
	return pool.allocateSegmentImmediately(ctx, defaultSegmentBlockSize)
}

func (pool *segmentManager) allocateSegmentImmediately(ctx context.Context, size int64) (*volume.SegmentBlock, error) {
	volIns := pool.selectorForSegmentCreate.Select(ctx, size)
	if volIns == nil {
		return nil, errors.New("no volume available")
	}
	segmentInfo, err := volIns.CreateSegment(ctx, size)
	if err != nil {
		return nil, err
	}
	return segmentInfo, nil
}

func (pool *segmentManager) dynamicAllocateSegmentTask() {
	//TODO
}

func (pool *segmentManager) generateSegmentBlockID() string {
	//TODO optimize
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (pool *segmentManager) createSegmentBlockReplicaGroup(segInfo *volume.SegmentBlock) error {
	// TODO implement
	segInfo.ReplicaGroupID = "group-1"
	segInfo.PeersAddress = []string{"ip1", "ip2"}
	// pick 2 segments with same capacity
	return nil
}

func (pool *segmentManager) getAppendableSegment(ctx context.Context,
	eli *info.EventLogInfo, num int) ([]*volume.SegmentBlock, error) {
	sl, exist := pool.eventLogSegment[eli.ID]
	if !exist {
		return nil, ErrEventLogNotFound
	}
	arr := make([]*volume.SegmentBlock, 0)
	next := sl.Front()
	hit := 0
	for hit < num && next != nil {
		sbi := next.Value.(*volume.SegmentBlock)
		next = next.Next()
		if sbi.IsFull {
			continue
		}
		hit++
		arr = append(arr, sbi)
	}

	if len(arr) == 0 {
		return pool.bindSegment(ctx, eli, 1)
	}
	return arr, nil
}

func (pool *segmentManager) updateSegment(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	for idx := range req.HealthInfo {
		hInfo := req.HealthInfo[idx]

		// TODO there is problem in data structure design OPTIMIZE
		v, exist := pool.segmentMap.Load(hInfo.Id)
		if !exist {
			log.Warning(ctx, "the segment not found when heartbeat", map[string]interface{}{
				"segment_id": hInfo.Id,
			})
			continue
		}
		in := v.(*volume.SegmentBlock)
		if hInfo.IsFull {
			in.IsFull = true

			next := pool.getSegmentBlockByID(ctx, in.NextSegmentId)
			if next != nil {
				next.StartOffsetInLog = in.StartOffsetInLog + int64(in.Number)
				if err := pool.updateSegmentBlockInKV(ctx, next); err != nil {
					log.Warning(ctx, "update the segment's start_offset failed ", map[string]interface{}{
						"segment_id":   hInfo.Id,
						"next_segment": next.ID,
						log.KeyError:   err,
					})
					return err
				}
			}
		}
		in.Size = hInfo.Size
		in.Number = hInfo.EventNumber
		if err := pool.updateSegmentBlockInKV(ctx, in); err != nil {
			log.Warning(ctx, "update the segment failed ", map[string]interface{}{
				"segment_id": hInfo.Id,
				log.KeyError: err,
			})
			return err
		}
	}
	return nil
}

func (pool *segmentManager) getEventLogSegmentList(elID string) []*volume.SegmentBlock {
	el, exist := pool.eventLogSegment[elID]
	if !exist {
		return nil
	}
	var arr []*volume.SegmentBlock
	next := el.Front()
	for next != nil {
		arr = append(arr, next.Value.(*volume.SegmentBlock))
		next = next.Next()
	}
	return arr
}

func (pool *segmentManager) getSegmentBlockByID(ctx context.Context, id string) *volume.SegmentBlock {
	v, exist := pool.segmentMap.Load(id)
	if !exist {
		return nil
	}

	return v.(*volume.SegmentBlock)
}

func (pool *segmentManager) getSegmentBlockKeyInKVStore(sbID string) string {
	return strings.Join([]string{segmentBlockKeyPrefixInKVStore, sbID}, "/")
}

func (pool *segmentManager) updateSegmentBlockInKV(ctx context.Context, segment *volume.SegmentBlock) error {
	if segment == nil {
		return nil
	}
	data, err := json.Marshal(segment)
	if err != nil {
		return err
	}
	return pool.kvStore.Set(ctx, pool.getSegmentBlockKeyInKVStore(segment.ID), data)
}

func (pool *segmentManager) getLastSegmentOfEventLog(el *info.EventLogInfo) *volume.SegmentBlock {
	sl := pool.eventLogSegment[el.ID]
	if sl == nil {
		return nil
	}
	val := sl.Back().Value
	if val == nil {
		return nil
	}
	return val.(*volume.SegmentBlock)
}
