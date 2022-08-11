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
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	// first-party libraries.
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/observability"
)

const (
	headerBlockSize = 4 * 1024
	// version + reserved(4) + capacity + entrySize + entryNum + indexNum + full.
	headerSize   = 4 + 4 + 8 + 8 + 4 + 4 + 1
	boundMinSize = indexSize
)

// blockMeta is mutable metadata of Block.
type blockMeta struct {
	// entrySize is the size of persisted entries.
	entrySize uint64
	// entryNum is the number of persisted entries.
	entryNum uint32
	// indexNum is the number of persisted indexes.
	indexNum uint32
	// full is the flag indicating Block is full.
	full bool
}

func (m *blockMeta) toAppendContext() appendContext {
	ctx := appendContext{
		offset: uint32(m.entrySize + headerBlockSize),
		num:    m.entryNum,
	}
	if m.full {
		ctx.full = 1
	}
	return ctx
}

// Block
//
// The layout of Block is:
//   ┌────────────────┬───────────────┬─────────┬─────----──────┐
//   │  Header Block  │  Entries ...  │  Bound  │  ... Indexes  │
//   └────────────────┴───────────────┴─────────┴────────----───┘
type Block struct {
	id    vanus.ID
	idStr string
	path  string

	version int32
	cap     int64

	fm      blockMeta // flushed meta
	actx    appendContext
	fi      uint32 // flushed index count
	indexes []index
	mu      sync.RWMutex

	f *os.File

	cis block.ClusterInfoSource
}

// Make sure block implements block.Block.
var _ block.Block = (*Block)(nil)

func (b *Block) ID() vanus.ID {
	return b.id
}

func (b *Block) Path() string {
	return b.path
}

func (b *Block) full() bool {
	return atomic.LoadUint32(&b.actx.full) != 0
}

func (b *Block) Appendable() bool {
	return !b.full()
}

func (b *Block) Close(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	// Flush metadata.
	if err := b.syncMeta(ctx); err != nil {
		return err
	}

	return b.f.Close()
}

func (b *Block) HealthInfo() *metapb.SegmentHealthInfo {
	// Copy append context.
	b.mu.RLock()
	actx := b.actx
	b.mu.RUnlock()

	// TODO(weihe.yin) add FirstEventBornTime & LastEventBornTime
	info := &metapb.SegmentHealthInfo{
		Id:                   b.id.Uint64(),
		Capacity:             b.cap,
		Size:                 int64(actx.size()),
		EventNumber:          int32(actx.num),
		SerializationVersion: b.version,
		IsFull:               actx.Full(),
	}

	// Fill cluster information.
	if cis := b.cis; cis != nil {
		cis.FillClusterInfo(info)
	}

	return info
}

func (b *Block) size() uint32 {
	return b.actx.size()
}

// remaining calculates remaining space by given entrySize and entryNum.
func (b *Block) remaining(size, num uint32) uint32 {
	// capacity - headerBlockSize - boundMinSize - dataLength - indexLength.
	return uint32(b.cap) - headerBlockSize - boundMinSize - size - num*indexSize
}

// syncMeta flushes metadata.
func (b *Block) syncMeta(ctx context.Context) error {
	if b.stale() {
		if err := b.persistHeader(ctx); err != nil {
			return err
		}
	}
	return nil
}

// stale checks if flushed metadata is stale.
func (b *Block) stale() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.fm.full != b.full() || b.fm.entrySize < uint64(b.actx.offset) || b.fm.indexNum < b.fi
}

func (b *Block) persistHeader(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	b.mu.RLock()
	meta := blockMeta{
		entrySize: uint64(b.actx.size()),
		entryNum:  b.actx.num,
		indexNum:  b.fi,
		full:      b.actx.Full(),
	}
	b.mu.RUnlock()

	var buf [headerSize]byte
	binary.BigEndian.PutUint32(buf[0:4], uint32(b.version))
	binary.BigEndian.PutUint64(buf[8:16], uint64(b.cap))
	binary.BigEndian.PutUint64(buf[16:24], meta.entrySize)
	binary.BigEndian.PutUint32(buf[24:28], meta.entryNum)
	binary.BigEndian.PutUint32(buf[28:32], meta.indexNum)
	if meta.full {
		buf[32] = 1
	}

	// TODO: does it safe when concurrent write and append?
	if _, err := b.f.WriteAt(buf[:], 0); err != nil {
		return err
	}

	b.mu.Lock()
	b.fm = meta
	b.mu.Unlock()

	return nil
}

func (b *Block) loadHeader(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	buf := make([]byte, headerSize)
	if _, err := b.f.ReadAt(buf, 0); err != nil {
		return err
	}

	b.version = int32(binary.BigEndian.Uint32(buf[0:4]))
	b.cap = int64(binary.BigEndian.Uint64(buf[8:16]))
	b.fm.entrySize = binary.BigEndian.Uint64(buf[16:24])
	b.fm.entryNum = binary.BigEndian.Uint32(buf[24:28])
	b.fm.indexNum = binary.BigEndian.Uint32(buf[28:32])
	b.fm.full = buf[32] != 0

	b.actx = b.fm.toAppendContext()
	b.fi = b.fm.indexNum

	return nil
}

func (b *Block) persistIndex(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if !b.full() {
		return nil
	}

	b.mu.RLock()
	indexes := b.indexes
	base := int(b.fi)
	b.mu.RUnlock()

	total := len(indexes)
	num := total - base
	size := num * indexSize
	buf := make([]byte, size)
	for i := 0; i < num; i++ {
		idx := &indexes[base+i]
		off := size - (i+1)*indexSize
		_, _ = idx.MarshalTo(buf[off : off+indexSize])
	}

	if _, err := b.f.WriteAt(buf, b.cap-int64(total)*indexSize); err != nil {
		return err
	}

	b.mu.Lock()
	b.fi = uint32(total)
	b.mu.Unlock()

	return nil
}

func (b *Block) loadIndex(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)

	if b.fi <= 0 {
		return nil
	}

	num := int(b.fi)
	size := num * indexSize

	// Read index data from file.
	data := make([]byte, size)
	if _, err := b.f.ReadAt(data, b.cap-int64(size)); err != nil {
		return err
	}

	// Decode indexes.
	b.indexes = make([]index, num, b.actx.num)
	for i := 0; i < num; i++ {
		off := size - (i+1)*indexSize
		b.indexes[i], _ = unmarshalIndex(data[off : off+indexSize])
	}

	return nil
}

func (b *Block) correctMeta() error {
	if len(b.indexes) == 0 && b.actx.num > 0 {
		b.indexes = make([]index, 0, b.actx.num)
	}

	// TODO(james.yin): scan indexes

	num := 0
	off := int64(headerBlockSize)
	if len(b.indexes) > 0 {
		num = len(b.indexes)
		off = b.indexes[num-1].EndOffset()
	}

	// Scan entries.
	buf := make([]byte, block.EntryLengthSize)
	for {
		if _, err := b.f.ReadAt(buf, off); err != nil {
			return err
		}

		length := block.EntryLength(buf)

		// Meet bound, stop scaning.
		if length == 0 {
			break
		}

		idx := index{
			offset: off,
			length: int32(length) + block.EntryLengthSize,
		}
		b.indexes = append(b.indexes, idx)

		off = idx.EndOffset()
		num++
	}

	// Reset meta data.
	b.actx.offset = uint32(off)
	b.actx.num = uint32(num)

	return nil
}

func (b *Block) validate(ctx context.Context) error {
	observability.EntryMark(ctx)
	defer observability.LeaveMark(ctx)
	return nil
}

func (b *Block) SetClusterInfoSource(cis block.ClusterInfoSource) {
	b.cis = cis
}

func (b *Block) Destroy(ctx context.Context) error {
	_ = b.Close(ctx)
	return os.Remove(b.path)
}
