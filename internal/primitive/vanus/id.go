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

package vanus

import (
	"strconv"
	"sync"
	"time"
)

type ID uint64

var (
	emptyID = ID(0)
	lock    = sync.Mutex{}
	base    = 10
	bitSize = 64
)

func EmptyID() ID {
	return emptyID
}
func NewID() ID {
	lock.Lock()
	defer lock.Unlock()
	// avoiding same id
	time.Sleep(time.Microsecond)
	return ID(time.Now().UnixNano())
}

func NewIDFromUint64(id uint64) ID {
	return ID(id)
}

func NewIDFromString(id string) (ID, error) {
	i, err := strconv.ParseUint(id, base, bitSize)
	if err != nil {
		return emptyID, err
	}
	return ID(i), nil
}

func (id ID) String() string {
	return strconv.FormatUint(uint64(id), base)
}

func (id ID) Uint64() uint64 {
	return uint64(id)
}

func (id ID) Key() string {
	return id.String()
}

func (id ID) Equals(cID ID) bool {
	return id.Uint64() == cID.Uint64()
}
