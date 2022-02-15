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
	"errors"
	"sync"
)

var (
	ErrNoCapacityLeft = errors.New("no capacity left")
)

type RingBuffer interface {
	BatchPut(...interface{}) error
	BatchGet(int) []interface{}
	RemoveFromHead(int)
	Length() int
	Capacity() int
	IsFull() bool
}

func NewRingBuffer(cap int) RingBuffer {
	return &ringBuffer{
		data: make([]interface{}, cap),
		cap:  cap,
	}
}

type ringBuffer struct {
	data    []interface{}
	head    int
	tail    int
	cap     int
	length  int
	rwMutex sync.RWMutex
}

func (r *ringBuffer) BatchPut(entries ...interface{}) error {
	length := len(entries)
	if length == 0 {
		return nil
	}
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	if r.availableCap() < length {
		return ErrNoCapacityLeft
	}
	// TODO does need deep copy?
	offset := 0
	for idx := range entries {
		if (r.tail+idx)%r.cap == 0 {
			r.tail = (r.tail + idx) % r.cap
			offset = idx
		}
		r.data[r.tail+idx-offset] = entries[idx]
	}
	r.tail = (r.tail + length) % r.cap
	r.length += length
	return nil
}

func (r *ringBuffer) BatchGet(length int) []interface{} {
	if length == 0 {
		return []interface{}{}
	}
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	if length > r.length {
		length = r.length
	}
	if length == 0 {
		return []interface{}{}
	}
	data := make([]interface{}, length)
	if r.head+length > r.cap {
		remain := r.cap - r.head
		copy(data[:remain], r.data[r.head:])
		copy(data[remain:], r.data[0:length-remain])
	} else {
		copy(data, r.data[r.head:r.head+length])
	}
	return data
}

func (r *ringBuffer) RemoveFromHead(length int) {
	if length == 0 {
		return
	}
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	if length > r.length {
		length = r.length
	}
	pos := r.head
	for idx := 0; idx < length; idx++ {
		pos = (idx + r.head) % r.cap
		r.data[pos] = nil
	}
	r.length -= length
	r.head = (pos + 1) % r.cap
}

func (r *ringBuffer) Length() int {
	r.rwMutex.RLock()
	defer r.rwMutex.Unlock()
	return r.length
}

func (r *ringBuffer) Capacity() int {
	return r.cap
}

func (r *ringBuffer) IsFull() bool {
	return r.Length() == r.cap
}

func (r *ringBuffer) availableCap() int {
	return r.cap - r.length
}
