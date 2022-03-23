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
	"github.com/linkall-labs/vanus/internal/trigger/info"
	"github.com/linkall-labs/vanus/observability/log"
	"sync"
)

type Manager struct {
	subOffset map[string]*SubscriptionOffset
	lock      sync.Mutex
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

func (m *Manager) RemoveSubscription(subId string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.subOffset, subId)
}

func (m *Manager) getCommit() map[string][]info.OffsetInfo {
	commit := make(map[string][]info.OffsetInfo, len(m.subOffset))
	for subId, offset := range m.subOffset {
		commit[subId] = offset.getCommit()
	}
	return commit
}

type SubscriptionOffset struct {
	subId    string
	elOffset map[string]*offsetTracker
	lock     sync.RWMutex
}

func (offset *SubscriptionOffset) RegisterEventLog(el string, currOffset int64) {
	offset.lock.Lock()
	defer offset.lock.Unlock()
	if _, exist := offset.elOffset[el]; !exist {
		offset.elOffset[el] = initOffset(currOffset)
	}
}

func (offset *SubscriptionOffset) ResetOffset(el string, currOffset int64) {
	offset.lock.Lock()
	defer offset.lock.Unlock()
	if o, exist := offset.elOffset[el]; exist {
		log.Info(nil, "offset reset", map[string]interface{}{
			log.KeySubscriptionID: offset.subId,
			log.KeyEventlogID:     el,
			"old":                 o.offsetToCommit(),
			"new":                 currOffset,
		})
	}
	offset.elOffset[el] = initOffset(currOffset)
}

func (offset *SubscriptionOffset) getOffsetTracker(el string) *offsetTracker {
	offset.lock.RLock()
	defer offset.lock.RUnlock()
	return offset.elOffset[el]
}

func (offset *SubscriptionOffset) EventReceive(record info.OffsetInfo) {
	r := offset.getOffsetTracker(record.EventLog)
	if r == nil {
		return
	}
	r.putOffset(record.Offset)
}

func (offset *SubscriptionOffset) EventCommit(record info.OffsetInfo) {
	r := offset.getOffsetTracker(record.EventLog)
	if r == nil {
		return
	}
	r.commitOffset(record.Offset)
}

func (offset *SubscriptionOffset) getCommit() []info.OffsetInfo {
	commit := make([]info.OffsetInfo, len(offset.elOffset))
	for el, tracker := range offset.elOffset {
		commit = append(commit, info.OffsetInfo{
			EventLog: el,
			Offset:   tracker.offsetToCommit(),
		})
	}
	return commit
}

type offsetTracker struct {
	mutex     sync.Mutex
	commit    int64
	maxOffset int64
	list      *skiplist.SkipList
}

func initOffset(initOffset int64) *offsetTracker {
	return &offsetTracker{
		commit:    initOffset,
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

func (o *offsetTracker) setCommit(commit int64) {
	o.commit = commit
}

func (o *offsetTracker) getCommit() int64 {
	return o.commit
}

func (o *offsetTracker) offsetToCommit() int64 {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.list.Len() == 0 {
		return o.maxOffset
	}
	return o.list.Front().Key().(int64)
}
