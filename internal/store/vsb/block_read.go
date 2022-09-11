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

package vsb

import (
	// standard libraries.
	"context"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
)

// Make sure block implements block.Reader.
var _ block.Reader = (*vsBlock)(nil)

// Read date from file.
func (b *vsBlock) Read(ctx context.Context, seq int64, num int) ([]block.Entry, error) {
	_, span := b.tracer.Start(ctx, "Read")
	defer span.End()

	from, to, num, err := b.entryRange(int(seq), num)
	if err != nil {
		return nil, err
	}

	length := int(to - from)
	data := make([]byte, length)
	if _, err = b.f.ReadAt(data, from); err != nil {
		return nil, err
	}

	entries := make([]block.Entry, 0, num)
	for so := 0; so < length; {
		n, entry, _ := b.dec.Unmarshal(data[so:])
		entries = append(entries, entry)
		so += n
	}

	return entries, nil
}

func (b *vsBlock) entryRange(start, num int) (int64, int64, int, error) {
	// TODO(james.yin): optimize lock.
	b.mu.RLock()
	defer b.mu.RUnlock()

	sz := len(b.indexes)

	if start >= sz {
		if start == sz && !b.full() {
			return -1, -1, 0, block.ErrOnEnd
		}
		return -1, -1, 0, block.ErrExceeded
	}

	end := start + num - 1
	if end >= sz {
		end = sz - 1
	}

	return b.indexes[start].StartOffset(), b.indexes[end].EndOffset(), end - start + 1, nil
}
