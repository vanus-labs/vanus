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
	"errors"
	"github.com/google/uuid"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/allocator"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/volume"
	"github.com/linkall-labs/vanus/internal/kv"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrEventLogNotFound = errors.New("eventlog not found")
)

const (
	defaultAutoCreatedSegmentNumber = 3
	eventlogKeyPrefixInKVStore      = "/vanus/internal/resource/eventlog"
)

type Manager interface {
	Init(ctx context.Context, kvClient kv.Client) error
	AcquireEventLog(ctx context.Context) (*info.EventLogInfo, error)
	UpdateEventLog(ctx context.Context, els ...*info.EventLogInfo) error
	GetEventLog(ctx context.Context, id string) *info.EventLogInfo
	GetEventLogSegmentList(elID string) []*allocator.Block
	GetAppendableSegment(ctx context.Context, eli *info.EventLogInfo,
		num int) ([]*allocator.Block, error)
	UpdateSegment(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error
}

type eventlogManager struct {
	kvStore          kv.Client
	segmentAllocator allocator.Allocator
	// string, *info.EventLogInfo
	eventLogMap      sync.Map
	boundEventLogMap sync.Map
	freeEventLogMap  *skiplist.SkipList
	kvMutex          sync.Mutex
	volMgr           volume.Manager
	kvClient         kv.Client
	logMap           map[string]*skiplist.SkipList
}

func NewManager(volMgr volume.Manager) Manager {
	return &eventlogManager{
		volMgr:          volMgr,
		freeEventLogMap: skiplist.New(skiplist.String),
	}
}

func (mgr *eventlogManager) Init(ctx context.Context, kvClient kv.Client) error {
	mgr.kvClient = kvClient
	mgr.segmentAllocator = allocator.NewAllocator(mgr.volMgr, allocator.NewVolumeRoundRobin(mgr.volMgr.GetAllVolume))
	if err := mgr.segmentAllocator.Init(ctx, mgr.kvStore); err != nil {
		return err
	}
	pairs, err := mgr.kvStore.List(ctx, eventlogKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		elInfo := &info.EventLogInfo{}
		err := json.Unmarshal(pair.Value, elInfo)
		if err != nil {
			return err
		}
		mgr.eventLogMap.Store(filepath.Base(pair.Key), elInfo)
	}
	return mgr.initVolumeInfo(ctx)
}

func (mgr *eventlogManager) initVolumeInfo(ctx context.Context) error {
	var err error
	//ctx := context.Background()
	mgr.eventLogMap.Range(func(key, value interface{}) bool {
		elInfo := value.(*info.EventLogInfo)
		sbList := mgr.GetEventLogSegmentList(elInfo.ID)
		for idx := 0; idx < len(sbList); idx++ {
			//sb := sbList[idx]
			//sb.VolumeMeta = *(volumeMgr.GetVolumeByID(sb.VolumeID).GetMeta())
		}
		return true
	})
	return err
}

func (mgr *eventlogManager) stop(ctx context.Context) {
}

func (mgr *eventlogManager) AcquireEventLog(ctx context.Context) (*info.EventLogInfo, error) {
	ele := mgr.freeEventLogMap.Front()
	var el *info.EventLogInfo
	if ele == nil {
		_el, err := mgr.createEventLog(ctx)
		if err != nil {
			return nil, err
		}
		el = _el
	} else {
		el = ele.Value.(*info.EventLogInfo)
	}
	mgr.boundEventLogMap.Store(el.ID, el)
	if err := mgr.initializeEventLog(ctx, el); err != nil {
		return nil, err
	}
	return el, nil
}

func (mgr *eventlogManager) createEventLog(ctx context.Context) (*info.EventLogInfo, error) {
	el := &info.EventLogInfo{
		// TODO use new uuid generator
		ID: uuid.NewString(),
	}
	data, _ := json.Marshal(el)
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	if err := mgr.kvStore.Set(ctx, mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
		return nil, err
	}
	mgr.eventLogMap.Store(el.ID, el)
	return el, nil
}

func (mgr *eventlogManager) GetEventLog(ctx context.Context, id string) *info.EventLogInfo {
	v, exist := mgr.eventLogMap.Load(id)

	if exist {
		return v.(*info.EventLogInfo)
	}
	return nil
}

func (mgr *eventlogManager) UpdateEventLog(ctx context.Context, els ...*info.EventLogInfo) error {
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	for idx := range els {
		el := els[idx]
		data, _ := json.Marshal(el)
		if err := mgr.kvStore.Set(ctx, mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *eventlogManager) initializeEventLog(ctx context.Context, el *info.EventLogInfo) error {
	_, err := mgr.segmentAllocator.Pick(ctx, defaultAutoCreatedSegmentNumber, 64*1024*1024)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *eventlogManager) dynamicScaleUpEventLog() error {
	return nil
}

func (mgr *eventlogManager) getEventLogKeyInKVStore(elName string) string {
	return strings.Join([]string{eventlogKeyPrefixInKVStore, elName}, "/")
}

func (mgr *eventlogManager) GetAppendableSegment(ctx context.Context,
	eli *info.EventLogInfo, num int) ([]*allocator.Block, error) {
	// TODO the HA of allocator can't be guaranteed before allocator support multiple replicas
	sl := mgr.logMap[eli.ID]
	if sl == nil {
		return nil, ErrEventLogNotFound
	}
	arr := make([]*allocator.Block, 0)
	next := sl.Front()
	hit := 0
	for hit < num && next != nil {
		sbi := next.Value.(*allocator.Block)
		next = next.Next()
		if sbi.IsFull {
			continue
		}
		hit++
		arr = append(arr, sbi)
	}

	if len(arr) == 0 {
		// TODO
		//return mgr.bindSegment(ctx, eli, 1)
	}
	return arr, nil
}

func (mgr *eventlogManager) UpdateSegment(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	//for idx := range req.HealthInfo {
	//hInfo := req.HealthInfo[idx]

	//// TODO there is problem in data structure design OPTIMIZE
	//v, exist := mgr.segmentMap.Load(hInfo.Id)
	//if !exist {
	//	log.Warning(ctx, "the allocator not found when heartbeat", map[string]interface{}{
	//		"segment_id": hInfo.Id,
	//	})
	//	continue
	//}
	//in := v.(*allocator.Segment)
	//if hInfo.IsFull {
	//	in.IsFull = true
	//
	//	next := mgr.getSegmentBlockByID(ctx, in.NextSegmentId)
	//	if next != nil {
	//		next.StartOffsetInLog = in.StartOffsetInLog + int64(in.Number)
	//		if err := mgr.updateSegmentBlockInKV(ctx, next); err != nil {
	//			log.Warning(ctx, "update the allocator's start_offset failed ", map[string]interface{}{
	//				"segment_id":   hInfo.Id,
	//				"next_segment": next.ID,
	//				log.KeyError:   err,
	//			})
	//			return err
	//		}
	//	}
	//}
	//in.Size = hInfo.Size
	//in.Number = hInfo.EventNumber
	//if err := mgr.updateSegmentBlockInKV(ctx, in); err != nil {
	//	log.Warning(ctx, "update the allocator failed ", map[string]interface{}{
	//		"segment_id": hInfo.Id,
	//		log.KeyError: err,
	//	})
	//	return err
	//}
	//}
	return nil
}

func (mgr *eventlogManager) GetEventLogSegmentList(elID string) []*allocator.Block {
	el := mgr.logMap[elID]
	if el == nil {
		return nil
	}
	var arr []*allocator.Block
	next := el.Front()
	for next != nil {
		arr = append(arr, next.Value.(*allocator.Block))
		next = next.Next()
	}
	return arr
}
