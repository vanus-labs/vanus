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
	"fmt"
	"github.com/sony/sonyflake"
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

func New() {

}

var (
	generator *sonyflake.Sonyflake
	once      sync.Once
)

func InitSnowflake(ctrlAddr []string, nodeID uint16) error {
	once.Do(func() {
		sonyflake.NewSonyflake(sonyflake.Settings{
			StartTime: time.Time{},
			MachineID: func() (uint16, error) {
				return nodeID, nil
			},
			CheckMachineID: nil,
		})
	})
	return nil
}

func NewID() (ID, error) {
	lock.Lock()
	defer lock.Unlock()

	id, err := generator.NextID()
	if err != nil {
		return EmptyID(), err
	}
	return ID(id), nil
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
	return fmt.Sprintf("%X", uint64(id))
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
