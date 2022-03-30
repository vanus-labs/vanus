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
	subOffset      map[string]*SubscriptionOffset
	lock           sync.Mutex
	lastCommitTime time.Time
}

func NewOffsetManager() *Manager {
	return &Manager{
		subOffset: map[string]*SubscriptionOffset{},
	}
}

func (m *Manager) RegisterSubscription(subId string) *SubscriptionOffset {
	m.lock.Lock()
	defer m.lock.Unlock()
	sub, exist := m.subOffset[subId]
	if !exist {
		sub = &SubscriptionOffset{
			subId:    subId,
			elOffset: map[string]*offsetTracker{},
		}
		m.subOffset[subId] = sub
	}
	return sub
}

func (m *Manager) GetSubscription(subId string) *SubscriptionOffset {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.subOffset[subId]
}

func (m *Manager) RemoveSubscription(subId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.subOffset, subId)
}

func (m *Manager) SetLastCommitTime() {
	m.lastCommitTime = time.Now()
}

func (m *Manager) GetLastCommitTime() time.Time {
	return m.lastCommitTime
}

type SubscriptionOffset struct {
	subId    string
	elOffset map[string]*offsetTracker
	lock     sync.RWMutex
}

func (offset *SubscriptionOffset) getElOffset() map[string]*offsetTracker {
	offset.lock.RLock()
	defer offset.lock.RUnlock()
	return offset.elOffset
}

func (offset *SubscriptionOffset) getOffsetTracker(el string) *offsetTracker {
	offset.lock.RLock()
	defer offset.lock.RUnlock()
	return offset.elOffset[el]
}

func (offset *SubscriptionOffset) initOffsetTracker(info info.OffsetInfo) *offsetTracker {
	offset.lock.Lock()
	defer offset.lock.Unlock()
	o, exist := offset.elOffset[info.EventLog]
	if !exist {
		o = initOffset(info.Offset)
		offset.elOffset[info.EventLog] = o
	}
	return o
}

func (offset *SubscriptionOffset) EventReceive(info info.OffsetInfo) {
	r := offset.getOffsetTracker(info.EventLog)
	if r == nil {
		r = offset.initOffsetTracker(info)
	}
	r.putOffset(info.Offset)
}

func (offset *SubscriptionOffset) EventCommit(info info.OffsetInfo) {
	r := offset.getOffsetTracker(info.EventLog)
	if r == nil {
		return
	}
	r.commitOffset(info.Offset)
}

func (offset *SubscriptionOffset) GetCommit() []info.OffsetInfo {
	var commit []info.OffsetInfo
	elOffset := offset.getElOffset()
	for el, tracker := range elOffset {
		commit = append(commit, info.OffsetInfo{
			EventLog: el,
			Offset:   tracker.offsetToCommit(),
		})
	}
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
	return o.list.Front().Key().(int64)
}
