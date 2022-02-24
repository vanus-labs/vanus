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
	"github.com/linkall-labs/vsproto/pkg/meta"
)

type segmentPool struct {
}

func (pool *segmentPool) init() error {
	go pool.allocateSegmentTask()
	return nil
}

func (pool *segmentPool) destroy() error {
	return nil
}

func (pool *segmentPool) bindSegment(ctx context.Context, el *meta.EventLog, num int) ([]*meta.Segment, error) {
	segArr := make([]*meta.Segment, 0)
	var err error
	defer func() {
		for idx := 0; idx < num; idx++ {
			if err != nil && segArr[idx] != nil {
				pool.cancelBinding(segArr[idx])
			}
		}
	}()
	for idx := 0; idx < num; idx++ {
		// TODO eliminate magic code
		seg := pool.pickSegment(64 * 1024 * 1024)
		if seg == nil {
			// no enough segment, manually allocate and bind
			seg, err = pool.allocateAndBindImmediately(ctx, el)
			if err != nil {
				return nil, err
			}
		}
		segArr[idx] = seg
	}

	return segArr, nil
}

func (pool *segmentPool) addSegmentServer(info *SegmentServerInfo) error {
	return nil
}

func (pool *segmentPool) removeSegmentServer(info *SegmentServerInfo) error {
	return nil
}

func (pool *segmentPool) pickSegment(size int64) *meta.Segment {
	return nil
}

func (pool *segmentPool) allocateAndBindImmediately(ctx context.Context, el *meta.EventLog) (*meta.Segment, error) {
	return nil, nil
}

func (pool *segmentPool) allocateSegmentTask() {

}

func (pool *segmentPool) cancelBinding(segment *meta.Segment) {
	if segment == nil {
		return
	}
}
