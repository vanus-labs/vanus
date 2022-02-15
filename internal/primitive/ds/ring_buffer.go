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
	rwMutex sync.RWMutex
}

func (r *ringBuffer) BatchPut(entries ...interface{}) error {
	l := len(entries)
	if l == 0 {
		return nil
	}
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	if r.availableCap() < l {
		return ErrNoCapacityLeft
	}
	// TODO does need deep copy?
	for idx := range entries {
		r.data[r.tail+idx] = entries[idx]
	}
	r.tail += l
	return nil
}

func (r *ringBuffer) BatchGet(length int) []interface{} {
	if length == 0 {
		return []interface{}{}
	}
	r.rwMutex.RLock()
	defer r.rwMutex.RUnlock()
	if length > r.bufferLen() {
		length = r.bufferLen()
	}
	data := make([]interface{}, length)
	copy(data, r.data[r.head:r.head+length])
	return data
}

func (r *ringBuffer) RemoveFromHead(length int) {
	r.rwMutex.Lock()
	defer r.rwMutex.Unlock()
	if length > r.bufferLen() {
		length = r.bufferLen()
	}
	for idx := 0; idx < length; idx++ {
		r.data[idx+r.head] = nil
	}
	r.head += length
}

func (r *ringBuffer) Length() int {
	r.rwMutex.RLock()
	defer r.rwMutex.Unlock()
	return r.bufferLen()
}

func (r *ringBuffer) Capacity() int {
	return r.cap
}

func (r *ringBuffer) IsFull() bool {
	return r.Length() == r.cap
}

func (r *ringBuffer) bufferLen() int {
	if r.tail >= r.head {
		return r.tail - r.head + 1
	} else {
		return r.cap - r.head + r.tail + 1
	}
}

func (r *ringBuffer) availableCap() int {
	return r.Capacity() - r.Length()
}
