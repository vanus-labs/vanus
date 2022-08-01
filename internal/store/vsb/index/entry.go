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

package index

import (
	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

type entry struct {
	block.EmptyEntryExt
	indexes []Index
}

// Make sure Entry implements block.EntryExt.
var _ block.EntryExt = (*entry)(nil)

func NewEntry(indexes []Index) block.Entry {
	return &entry{
		indexes: indexes,
	}
}

func (e *entry) Get(ordinal int) interface{} {
	if ordinal == ceschema.IndexesOrdinal {
		return e.indexes
	}
	if ordinal >= 0 && ordinal < len(e.indexes) {
		return e.indexes[ordinal]
	}
	return e.EmptyEntry.Get(ordinal)
}

func (e *entry) GetUint16(ordinal int) uint16 {
	if ordinal == ceschema.EntryTypeOrdinal {
		return ceschema.Index
	}
	return e.EmptyEntry.GetUint16(ordinal)
}

func (e *entry) OptionalAttributeCount() int {
	return len(e.indexes)
}

func (e *entry) RangeOptionalAttributes(f func(ordinal int, val interface{})) {
	for i, idx := range e.indexes {
		f(i, idx)
	}
}
