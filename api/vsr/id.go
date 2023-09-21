// Copyright 2023 Linkall Inc.
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

// vanus resource.
package vsr

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	emptyID = ID(0)
	base    = 16
	bitSize = 64
)

var ErrEmptyID = errors.New("id: empty")

type ID uint64

func EmptyID() ID {
	return emptyID
}

func NewIDFromUint64(id uint64) ID {
	return ID(id)
}

func NewIDFromString(id string) (ID, error) {
	if id == "" {
		return emptyID, ErrEmptyID
	}
	i, err := strconv.ParseUint(id, base, bitSize)
	if err != nil {
		return emptyID, err
	}
	return ID(i), nil
}

func (id ID) String() string {
	return fmt.Sprintf("%016X", uint64(id))
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

type IDList []ID

func (l IDList) Contains(id ID) bool {
	for i := range l {
		if id.Equals(l[i]) {
			return true
		}
	}
	return false
}
