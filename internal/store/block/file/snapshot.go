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
	// this project.
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/store/block"
)

// Make sure block implements raftlog.SnapshotOperator.
var _ raftlog.SnapshotOperator = (*Block)(nil)

func (b *Block) GetSnapshot(index uint64) ([]byte, error) {
	b.mu.RLock()
	if len(b.indexes) == 0 {
		b.mu.RUnlock()
		return []byte{}, nil
	}
	so := b.indexes[0].StartOffset()
	eo := b.indexes[len(b.indexes)-1].EndOffset()
	b.mu.RUnlock()

	data := make([]byte, eo-so)
	if _, err := b.f.ReadAt(data, so); err != nil {
		return nil, err
	}
	return data, nil
}

func (b *Block) ApplySnapshot(data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var eo int64
	if sz := len(b.indexes); sz != 0 {
		eo = b.indexes[sz-1].EndOffset()
	} else {
		eo = int64(headerBlockSize)
	}

	if int64(len(data)+headerBlockSize) <= eo {
		return nil
	}

	if _, err := b.f.WriteAt(data[eo-int64(headerBlockSize):], eo); err != nil {
		return err
	}

	// Build indexes from data.
	for off := eo; off < int64(len(data)+headerBlockSize); {
		length := block.EntryLength(data[off-int64(headerBlockSize):])

		idx := index{
			offset: off,
			length: int32(length) + block.EntryLengthSize,
		}
		b.indexes = append(b.indexes, idx)

		off = idx.EndOffset()
	}
	b.actx.num = uint32(len(b.indexes))
	b.actx.offset = uint32(len(data) + headerBlockSize)

	return nil
}
