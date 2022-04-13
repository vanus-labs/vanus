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
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"sync"
	"time"
)

type Manager struct {
	subOffset      sync.Map
	lastCommitTime time.Time
}

func NewOffsetManager() *Manager {
	return &Manager{}
}

func (m *Manager) RegisterSubscription(subId string) *SubscriptionOffset {
	subOffset, exist := m.subOffset.Load(subId)
	if !exist {
		sub := &SubscriptionOffset{
			subId: subId,
		}
		subOffset, _ = m.subOffset.LoadOrStore(subId, sub)
	}
	return subOffset.(*SubscriptionOffset)
}

func (m *Manager) GetSubscription(subId string) *SubscriptionOffset {
	sub, exist := m.subOffset.Load(subId)
	if !exist {
		return nil
	}
	return sub.(*SubscriptionOffset)
}

func (m *Manager) RemoveSubscription(subId string) {
	m.subOffset.Delete(subId)
}

func (m *Manager) SetLastCommitTime() {
	m.lastCommitTime = time.Now()
}

func (m *Manager) GetLastCommitTime() time.Time {
	return m.lastCommitTime
}

type SubscriptionOffset struct {
	subId    string
	elOffset sync.Map
}

func (offset *SubscriptionOffset) EventReceive(info info.OffsetInfo) {
	o, exist := offset.elOffset.Load(info.EventLogId)
	if !exist {
		o, _ = offset.elOffset.LoadOrStore(info.EventLogId, initOffset(info.Offset))
	}
	o.(*offsetTracker).putOffset(info.Offset)
}

func (offset *SubscriptionOffset) EventCommit(info info.OffsetInfo) {
	o, exist := offset.elOffset.Load(info.EventLogId)
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
			EventLogId: key.(string),
			Offset:     tracker.offsetToCommit(),
		})
		return true
	})
	return commit
}

type offsetTracker struct {
	mutex     sync.Mutex
	maxOffset int64
	list      *skiplist.SkipList
}

func initOffset(initOffset int64) *offsetTracker {
	return &offsetTracker{
		maxOffset: initOffset,
		list: skiplist.New(skiplist.GreaterThanFunc(func(lhs, rhs interface{}) int {
			v1 := lhs.(int64)
			v2 := rhs.(int64)
			if v1 > v2 {
				return 1
			} else if v1 < v2 {
				return -1
			}
			return 0
		})),
	}
}

func (o *offsetTracker) putOffset(offset int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Set(offset, offset)
	o.maxOffset = o.list.Back().Key().(int64)
}

func (o *offsetTracker) commitOffset(offset int64) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.list.Remove(offset)
}

func (o *offsetTracker) offsetToCommit() int64 {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.list.Len() == 0 {
		return o.maxOffset
	}
	return o.list.Front().Key().(int64) - 1
}
