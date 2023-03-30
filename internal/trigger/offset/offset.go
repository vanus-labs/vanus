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

package offset

import (
	"sync"

	"github.com/huandu/skiplist"

	"github.com/vanus-labs/vanus/internal/primitive/info"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

func NewSubscriptionOffset(id vanus.ID, maxUACKNumber int, initOffsets info.ListOffsetInfo) *SubscriptionOffset {
	sub := &SubscriptionOffset{
		subscriptionID: id,
		cond:           sync.NewCond(&sync.Mutex{}),
		maxUACKNumber:  maxUACKNumber,
		elOffsets:      make(map[vanus.ID]*offsetTracker, len(initOffsets)),
	}
	for _, offset := range initOffsets {
		sub.elOffsets[offset.EventlogID] = initOffset(offset.Offset)
	}
	return sub
}

type SubscriptionOffset struct {
	subscriptionID vanus.ID
	cond           *sync.Cond
	maxUACKNumber  int
	uACKNumber     int
	elOffsets      map[vanus.ID]*offsetTracker
	closed         bool
}

func (offset *SubscriptionOffset) Close() {
	offset.cond.L.Lock()
	defer offset.cond.L.Unlock()
	offset.closed = true
	offset.cond.Broadcast()
}

func (offset *SubscriptionOffset) EventReceive(info info.OffsetInfo) {
	offset.cond.L.Lock()
	defer offset.cond.L.Unlock()
	for offset.uACKNumber >= offset.maxUACKNumber && !offset.closed {
		offset.cond.Wait()
	}
	if offset.closed {
		return
	}
	offset.uACKNumber++
	tracker, exist := offset.elOffsets[info.EventlogID]
	if !exist {
		tracker = initOffset(info.Offset)
		offset.elOffsets[info.EventlogID] = tracker
	}
	tracker.putOffset(info.Offset)
}

func (offset *SubscriptionOffset) EventCommit(info info.OffsetInfo) {
	offset.cond.L.Lock()
	defer offset.cond.L.Unlock()
	if offset.closed {
		return
	}
	tracker, exist := offset.elOffsets[info.EventlogID]
	if !exist {
		return
	}
	offset.uACKNumber--
	offset.cond.Signal()
	tracker.commitOffset(info.Offset)
}

func (offset *SubscriptionOffset) GetCommit() info.ListOffsetInfo {
	offset.cond.L.Lock()
	defer offset.cond.L.Unlock()
	var commit info.ListOffsetInfo
	for id, tracker := range offset.elOffsets {
		commit = append(commit, info.OffsetInfo{
			EventlogID: id,
			Offset:     tracker.offsetToCommit(),
		})
	}
	return commit
}

type offsetTracker struct {
	maxOffset int64
	list      *skiplist.SkipList
}

func initOffset(initOffset uint64) *offsetTracker {
	return &offsetTracker{
		maxOffset: int64(initOffset) - 1,
		list: skiplist.New(skiplist.GreaterThanFunc(func(lhs, rhs interface{}) int {
			v1, _ := lhs.(uint64)
			v2, _ := rhs.(uint64)
			if v1 > v2 {
				return 1
			} else if v1 < v2 {
				return -1
			}
			return 0
		})),
	}
}

func (o *offsetTracker) putOffset(offset uint64) {
	o.list.Set(offset, offset)
	if int64(offset) > o.maxOffset {
		o.maxOffset = int64(offset)
	}
}

func (o *offsetTracker) commitOffset(offset uint64) {
	o.list.Remove(offset)
}

func (o *offsetTracker) offsetToCommit() uint64 {
	if o.list.Len() == 0 {
		return uint64(o.maxOffset + 1)
	}
	return o.list.Front().Key().(uint64)
}
