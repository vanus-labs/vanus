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
	"time"

	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"

	"github.com/huandu/skiplist"
)

type Manager struct {
	subOffset      sync.Map
	lastCommitTime time.Time
}

func NewOffsetManager() *Manager {
	return &Manager{}
}

func (m *Manager) RegisterSubscription(id vanus.ID) *SubscriptionOffset {
	subOffset, exist := m.subOffset.Load(id)
	if !exist {
		sub := &SubscriptionOffset{
			subscriptionID: id,
		}
		subOffset, _ = m.subOffset.LoadOrStore(id, sub)
	}
	return subOffset.(*SubscriptionOffset)
}

func (m *Manager) GetSubscription(id vanus.ID) *SubscriptionOffset {
	sub, exist := m.subOffset.Load(id)
	if !exist {
		return nil
	}
	return sub.(*SubscriptionOffset)
}

func (m *Manager) RemoveSubscription(id vanus.ID) {
	m.subOffset.Delete(id)
}

func (m *Manager) SetLastCommitTime() {
	m.lastCommitTime = time.Now()
}

func (m *Manager) GetLastCommitTime() time.Time {
	return m.lastCommitTime
}

type SubscriptionOffset struct {
	subscriptionID vanus.ID
	elOffset       sync.Map
}

func (offset *SubscriptionOffset) EventReceive(info info.OffsetInfo) {
	o, exist := offset.elOffset.Load(info.EventLogID)
	if !exist {
		o, _ = offset.elOffset.LoadOrStore(info.EventLogID, initOffset(info.Offset))
	}
	o.(*offsetTracker).putOffset(info.Offset)
}

func (offset *SubscriptionOffset) EventCommit(info info.OffsetInfo) {
	o, exist := offset.elOffset.Load(info.EventLogID)
	if !exist {
		return
	}
	o.(*offsetTracker).commitOffset(info.Offset)
}

func (offset *SubscriptionOffset) GetCommit() info.ListOffsetInfo {
	var commit info.ListOffsetInfo
	offset.elOffset.Range(func(key, value interface{}) bool {
		tracker := value.(*offsetTracker)
		commit = append(commit, info.OffsetInfo{
			EventLogID: key.(vanus.ID),
			Offset:     tracker.offsetToCommit(),
		})
		return true
	})
	return commit
}

type offsetTracker struct {
	mutex     sync.Mutex
	maxOffset uint64
	list      *skiplist.SkipList
}

func initOffset(initOffset uint64) *offsetTracker {
	return &offsetTracker{
		maxOffset: initOffset,
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
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Set(offset, offset)
	o.maxOffset, _ = o.list.Back().Key().(uint64)
}

func (o *offsetTracker) commitOffset(offset uint64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Remove(offset)
}

func (o *offsetTracker) offsetToCommit() uint64 {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.list.Len() == 0 {
		return o.maxOffset
	}
	return o.list.Front().Key().(uint64) - 1
}
