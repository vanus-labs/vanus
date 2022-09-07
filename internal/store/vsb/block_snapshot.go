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
	"encoding/binary"
	"sync/atomic"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

// Make sure block implements block.Snapshoter.
var _ block.Snapshoter = (*vsBlock)(nil)

func (b *vsBlock) makeSnapshot() (meta, []index.Index) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return makeSnapshot(b.actx, b.indexes)
}

func makeSnapshot(actx appendContext, indexes []index.Index) (meta, []index.Index) {
	m := meta{
		writeOffset: actx.offset,
		archived:    actx.Archived(),
	}
	if sz := len(indexes); sz > 0 {
		m.entryLength = indexes[sz-1].EndOffset() - indexes[0].StartOffset()
		m.entryNum = int64(sz)
	}
	return m, indexes
}

func (b *vsBlock) Snapshot(ctx context.Context) (block.Fragment, error) {
	m, _ := b.makeSnapshot()

	if m.writeOffset == b.dataOffset {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(b.dataOffset))
		return block.NewFragment(buf), nil
	}

	data := make([]byte, m.writeOffset-b.dataOffset+8)
	binary.LittleEndian.PutUint64(data, uint64(b.dataOffset))

	if _, err := b.f.ReadAt(data[8:], b.dataOffset); err != nil {
		return nil, err
	}

	return block.NewFragment(data), nil
}

func (b *vsBlock) ApplySnapshot(ctx context.Context, snap block.Fragment) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	cur := b.actx.offset
	so := snap.StartOffset()
	if so > cur {
		return block.ErrSnapshotOutOfOrder
	}

	eo := snap.EndOffset()
	if eo <= cur {
		return nil
	}

	payload := snap.Payload()
	if _, err := b.f.WriteAt(payload[cur-snap.StartOffset():], cur); err != nil {
		return err
	}

	// Build indexes from data.
	for off := cur; off < eo; {
		n, entry, _ := b.dec.Unmarshal(payload[off-headerBlockSize:])

		if ceschema.EntryType(entry) == ceschema.End {
			atomic.StoreUint32(&b.actx.archived, 1)
			break
		}

		idx := index.NewIndex(off, int32(n), index.WithEntry(entry))
		b.indexes = append(b.indexes, idx)

		off += int64(n)
	}

	b.actx.seq = int64(len(b.indexes))
	b.actx.offset = eo

	return nil
}
