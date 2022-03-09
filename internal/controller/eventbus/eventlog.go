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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	ctrlpb "github.com/linkall-labs/vsproto/pkg/controller"
	"sync"
)

type eventlogManager struct {
	kvStore     kv.Client
	ctrl        *controller
	segmentPool *segmentPool
	eventLogMap sync.Map // string, *skiplist.SkipList
}

func newEventlogManager(ctrl *controller) *eventlogManager {
	return &eventlogManager{
		ctrl:    ctrl,
		kvStore: ctrl.kvStore,
	}
}

func (mgr *eventlogManager) start() error {
	mgr.segmentPool = newSegmentPool(mgr.ctrl)
	if err := mgr.segmentPool.init(); err != nil {
		return err
	}
	return nil
}

func (mgr *eventlogManager) stop() error {
	return nil
}

func (mgr *eventlogManager) acquireEventLog(ctx context.Context) (*info.EventLogInfo, error) {
	return nil, nil
}

func (mgr *eventlogManager) getEventLog(ctx context.Context, id string) *info.EventLogInfo {
	return nil
}

func (mgr *eventlogManager) updateEventLog(ctx context.Context, els ...*info.EventLogInfo) error {
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

func (mgr *eventlogManager) getEventLogSegmentList(elID string) []*info.SegmentBlockInfo {
	return nil
}

func (mgr *eventlogManager) updateSegment(ctx context.Context, req *ctrlpb.SegmentHeartbeatRequest) error {
	return mgr.segmentPool.updateSegment(ctx, req)
}

func (mgr *eventlogManager) getAppendableSegment(ctx context.Context,
	eli *info.EventLogInfo, num int) ([]*info.SegmentBlockInfo, error) {
	return mgr.segmentPool.getAppendableSegment(ctx, eli, num)
}
