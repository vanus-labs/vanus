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
	"fmt"
	"github.com/google/uuid"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/selector"
	"github.com/linkall-labs/vsproto/pkg/segment"
)

const (
	defaultSegmentBlockSize = 64 * 1024 * 1024
)

type segmentPool struct {
	ctrl                     *controller
	selectorForSegmentCreate selector.SegmentServerSelector
}

func (pool *segmentPool) init(ctrl *controller) error {
	pool.ctrl = ctrl
	go pool.dynamicAllocateSegmentTask()
	pool.selectorForSegmentCreate = selector.NewSegmentServerRoundRobinSelector(&ctrl.segmentServerInfoMap)
	return nil
}

func (pool *segmentPool) destroy() error {
	return nil
}

func (pool *segmentPool) bindSegment(ctx context.Context, el *info.EventLogInfo, num int) ([]*info.SegmentBlockInfo, error) {
	segArr := make([]*info.SegmentBlockInfo, num)
	var err error
	defer func() {
		for idx := 0; idx < num; idx++ {
			if err != nil && segArr[idx] != nil {
				pool.cancelBinding(segArr[idx])
			}
		}
	}()
	for idx := 0; idx < num; idx++ {
		seg, err := pool.pickSegment(ctx, defaultSegmentBlockSize)
		if err != nil {
			return nil, err
		}

		// binding, assign runtime fields
		seg.EventLogID = fmt.Sprintf("%d", el.ID)
		if err = pool.createSegmentBlockReplicaGroup(seg); err != nil {
			return nil, err
		}
		srvInfo := pool.ctrl.segmentServerInfoMap[seg.VolumeInfo.AssignedSegmentServer.ID()]
		client := pool.ctrl.getSegmentServerClient(srvInfo)
		_, err = client.ActiveSegmentBlock(ctx, &segment.ActiveSegmentBlockRequest{
			EventLogId:     seg.EventLogID,
			ReplicaGroupId: seg.ReplicaGroupID,
			PeersAddress:   seg.PeersAddress,
		})
		if err != nil {
			return nil, err
		}
		segArr[idx] = seg
	}
	// TODO persist to kv
	return segArr, nil
}

func (pool *segmentPool) pickSegment(ctx context.Context, size int64) (*info.SegmentBlockInfo, error) {
	// no enough segment, manually allocate and bind
	return pool.allocateSegmentImmediately(ctx, defaultSegmentBlockSize)
}

func (pool *segmentPool) allocateSegmentImmediately(ctx context.Context, size int64) (*info.SegmentBlockInfo, error) {
	srvInfo := pool.selectorForSegmentCreate.Select(ctx, size)
	client := pool.ctrl.getSegmentServerClient(srvInfo)
	segmentInfo := &info.SegmentBlockInfo{
		ID:         pool.generateSegmentBlockID(),
		Capacity:   size,
		VolumeInfo: srvInfo.Volume,
	}
	_, err := client.CreateSegmentBlock(ctx, &segment.CreateSegmentBlockRequest{
		Size: segmentInfo.Capacity,
		Id:   segmentInfo.ID,
	})
	if err != nil {
		return nil, err
	}
	// TODO persist to kv
	srvInfo.Volume.AddBlock(segmentInfo)
	return segmentInfo, nil
}

func (pool *segmentPool) dynamicAllocateSegmentTask() {

}

func (pool *segmentPool) cancelBinding(segment *info.SegmentBlockInfo) {
	if segment == nil {
		return
	}
}

func (pool *segmentPool) generateSegmentBlockID() string {
	//TODO optimize
	return uuid.NewString()
}

func (pool *segmentPool) createSegmentBlockReplicaGroup(segInfo *info.SegmentBlockInfo) error {
	// TODO implement
	segInfo.ReplicaGroupID = "group-1"
	segInfo.PeersAddress = []string{"ip1", "ip2"}
	// pick 2 segments with same capacity
	return nil
}
