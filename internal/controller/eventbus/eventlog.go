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
	"github.com/google/uuid"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"path/filepath"
	"strings"
	"sync"
)

type eventlogManager struct {
	kvStore     kv.Client
	ctrl        *controller
	segmentPool *segmentPool
	// string, *info.EventLogInfo
	eventLogMap      sync.Map
	boundEventLogMap sync.Map
	freeEventLogMap  *skiplist.SkipList
	kvMutex          sync.Mutex
}

func newEventlogManager(ctrl *controller) *eventlogManager {
	return &eventlogManager{
		ctrl:            ctrl,
		kvStore:         ctrl.kvStore,
		freeEventLogMap: skiplist.New(skiplist.String),
	}
}

func (mgr *eventlogManager) start() error {
	mgr.segmentPool = newSegmentPool(mgr.ctrl)
	if err := mgr.segmentPool.init(); err != nil {
		return err
	}
	pairs, err := mgr.kvStore.List(eventlogKeyPrefixInKVStore)
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
	return nil
}

func (mgr *eventlogManager) stop() error {
	return nil
}

func (mgr *eventlogManager) acquireEventLog(ctx context.Context) (*info.EventLogInfo, error) {
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
		ID:                    uuid.NewString(),
		CurrentSegmentNumbers: 0,
		SegmentList:           []string{},
	}
	data, _ := json.Marshal(el)
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	if err := mgr.kvStore.Set(mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
		return nil, err
	}
	mgr.eventLogMap.Store(el.ID, el)
	return el, nil
}

func (mgr *eventlogManager) getEventLog(ctx context.Context, id string) *info.EventLogInfo {
	v, exist := mgr.eventLogMap.Load(id)

	if exist {
		return v.(*info.EventLogInfo)
	}
	return nil
}

func (mgr *eventlogManager) updateEventLog(ctx context.Context, els ...*info.EventLogInfo) error {
	mgr.kvMutex.Lock()
	defer mgr.kvMutex.Unlock()
	for idx := range els {
		el := els[idx]
		data, _ := json.Marshal(el)
		if err := mgr.kvStore.Set(mgr.getEventLogKeyInKVStore(el.ID), data); err != nil {
			return err
		}
	}
	return nil
}

func (mgr *eventlogManager) initializeEventLog(ctx context.Context, el *info.EventLogInfo) error {
	_, err := mgr.segmentPool.bindSegment(ctx, el, defaultAutoCreatedSegmentNumber)
	if err != nil {
		return err
	}
	return nil
}

func (mgr *eventlogManager) dynamicScaleUpEventLog() error {
	return nil
}

func (mgr *eventlogManager) getEventLogSegmentList(ctx context.Context, elID string) []*info.SegmentBlockInfo {
	return mgr.segmentPool.getEventLogSegmentList(elID)
}

func (mgr *eventlogManager) updateSegment(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	return mgr.segmentPool.updateSegment(ctx, req)
}

func (mgr *eventlogManager) getAppendableSegment(ctx context.Context,
	eli *info.EventLogInfo, num int) ([]*info.SegmentBlockInfo, error) {
	return mgr.segmentPool.getAppendableSegment(ctx, eli, num)
}

func (mgr *eventlogManager) getEventLogKeyInKVStore(elName string) string {
	return strings.Join([]string{eventlogKeyPrefixInKVStore, elName}, "/")
}
