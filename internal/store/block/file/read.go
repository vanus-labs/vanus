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

package file

import (
	// standard libraries.
	"context"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/observability"
)

// Make sure block implements block.Reader.
var _ block.Reader = (*Block)(nil)

// Read date from file.
func (b *Block) Read(ctx context.Context, start, number int) ([]block.Entry, error) {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	from, to, num, err := b.entryRange(start, number)
	if err != nil {
		return nil, err
	}

	length := uint32(to - from)
	data := make([]byte, length)
	if _, err = b.f.ReadAt(data, from); err != nil {
		return nil, err
	}

	entries := make([]block.Entry, 0, num)
	for i, so, from2 := uint32(start), uint32(0), uint32(from); so < length; {
		entry, err2 := block.UnmarshalEntry(data[so:])
		if err2 != nil {
			return nil, err2
		}
		entry.Offset = from2 + so
		entry.Index = i
		entries = append(entries, entry)
		so += uint32(entry.Size())
		i++
	}

	return entries, nil
}

func (b *Block) entryRange(start, num int) (int64, int64, int, error) {
	// TODO(james.yin): optimize lock.
	b.mu.RLock()
	defer b.mu.RUnlock()

	sz := len(b.indexes)

	if start >= sz {
		if start == sz && !b.full() {
			return -1, -1, 0, block.ErrOffsetOnEnd
		}
		return -1, -1, 0, block.ErrOffsetExceeded
	}

	end := start + num - 1
	if end >= sz {
		end = sz - 1
	}

	return b.indexes[start].StartOffset(), b.indexes[end].EndOffset(), end - start + 1, nil
}
