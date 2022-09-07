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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, wther express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vsb

import (
	// standard libraries.

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

const (
	addedOptCount = 2
)

type entryExtWrapper struct {
	block.EntryExtWrapper
	t     uint16
	seq   int64
	stime int64
}

// Make sure entryWrapper implements block.Entry.
var _ block.EntryExt = (*entryExtWrapper)(nil)

func (w *entryExtWrapper) GetUint16(ordinal int) uint16 {
	if ordinal == ceschema.EntryTypeOrdinal {
		return w.t
	}
	return w.EntryExtWrapper.GetUint16(ordinal)
}

func (w *entryExtWrapper) GetInt64(ordinal int) int64 {
	switch ordinal {
	case ceschema.SequenceNumberOrdinal:
		return w.seq
	case ceschema.StimeOrdinal:
		return w.stime
	}
	return w.EntryExtWrapper.GetInt64(ordinal)
}

func (w *entryExtWrapper) RangeOptionalAttributes(f func(ordinal int, val interface{})) {
	f(ceschema.SequenceNumberOrdinal, w.seq)
	f(ceschema.StimeOrdinal, w.stime)
	w.EntryExtWrapper.RangeOptionalAttributes(f)
}

func (w *entryExtWrapper) OptionalAttributeCount() int {
	return addedOptCount + w.EntryExtWrapper.OptionalAttributeCount()
}

func wrapEntry(e block.Entry, t uint16, seq int64, stime int64) block.Entry {
	if ext, ok := e.(block.EntryExt); ok {
		return &entryExtWrapper{
			EntryExtWrapper: block.EntryExtWrapper{
				E: ext,
			},
			t:     t,
			seq:   seq,
			stime: stime,
		}
	}
	// TODO(james.yin): entry wrapper
	return nil
}
