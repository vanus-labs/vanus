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

package ds

import (
	"sort"
	"sync"
)

type SortedMap interface {
	Put(string, interface{})
	Get(string) Entry
	Head() Entry
	Tail() Entry
	Remove(string)
	Size() int
}

type sortedMap struct {
	data       map[string]Entry
	sortedKeys []string
	rwMutex    sync.RWMutex
	size       int
	head       Entry
	tail       Entry
}

func NewSortedMap() SortedMap {
	return &sortedMap{
		data:       make(map[string]Entry, 0),
		rwMutex:    sync.RWMutex{},
		sortedKeys: make([]string, 16),
	}
}

func (sm *sortedMap) Put(key string, val interface{}) {
	sm.rwMutex.Lock()
	defer sm.rwMutex.Unlock()

	e := &entry{
		key: key,
		val: val,
	}
	sm.data[key] = e
	if sm.size == 0 {
		sm.sortedKeys[0] = key
		sm.head = e
		sm.tail = e
		sm.size = 1
		return
	}

	// scale up capacity
	if len(sm.sortedKeys) == cap(sm.sortedKeys) {
		newKeys := make([]string, len(sm.data)*2)
		copy(newKeys, sm.sortedKeys)
		sm.sortedKeys = newKeys
	}
	sm.sortedKeys[sm.size] = key
	sm.size += 1
	sort.Strings(sm.sortedKeys)
	idx := 0
	for ; idx < sm.size; idx++ {
		if sm.sortedKeys[idx] == key {
			break
		}
	}

	if idx > 0 {
		pre := sm.data[sm.sortedKeys[idx-1]].(*entry)
		e.previous = pre
		e.next = pre.next
		if pre.next != nil {
			pre.next.(*entry).previous = e
		}
		pre.next = e
	} else {
		pre := sm.data[sm.sortedKeys[0]].(*entry)
		e.next = pre
		pre.previous = e
	}
	if e.previous == nil {
		sm.head = e
	}
	if e.next == nil {
		sm.tail = e
	}
}

func (sm *sortedMap) Get(key string) Entry {
	sm.rwMutex.RLock()
	defer sm.rwMutex.RUnlock()
	return sm.data[key]
}

func (sm *sortedMap) Head() Entry {
	sm.rwMutex.RLock()
	defer sm.rwMutex.RUnlock()
	return sm.head
}

func (sm *sortedMap) Tail() Entry {
	sm.rwMutex.RLock()
	defer sm.rwMutex.RUnlock()
	return sm.tail
}

func (sm *sortedMap) Remove(key string) {
	sm.rwMutex.Lock()
	defer sm.rwMutex.Unlock()
	e := sm.data[key]
	if e == nil {
		return
	}
	if sm.size == 0 {
		return
	}
	delete(sm.data, e.Key())
	_e := e.(*entry)

	idx := 0
	for ; idx < sm.size; idx++ {
		if sm.sortedKeys[idx] == e.Key() {
			break
		}
	}
	sm.sortedKeys[idx] = ""
	sm.size -= 1

	if idx > 0 {
		pre := sm.data[sm.sortedKeys[idx-1]].(*entry)
		next := sm.data[sm.sortedKeys[idx+1]]

		pre.next = next
		if next != nil {
			next.(*entry).previous = pre
		}
	} else {
		next := sm.data[sm.sortedKeys[1]]
		if next != nil {
			next.(*entry).previous = nil
		}
	}
	sort.Strings(sm.sortedKeys)

	if _e.previous == nil {
		sm.head = _e.next
	}

	if _e.next == nil {
		sm.tail = _e.previous
	}
}

func (sm *sortedMap) Size() int {
	return sm.size
}

type Entry interface {
	Previous() Entry
	Next() Entry
	Key() string
	Value() interface{}
}

type entry struct {
	previous Entry
	next     Entry
	key      string
	val      interface{}
	// TODO does need a mutex?
}

func (e *entry) Previous() Entry {
	return e.previous
}

func (e *entry) Next() Entry {
	return e.next
}

func (e *entry) Key() string {
	return e.key
}

func (e *entry) Value() interface{} {
	return e.val
}
