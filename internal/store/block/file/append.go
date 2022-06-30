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
	"sync/atomic"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/segment/errors"
	"github.com/linkall-labs/vanus/observability/log"
)

type appendContext struct {
	offset uint32
	num    uint32
	full   uint32
}

// Make sure appendContext implements block.AppendContext.
var _ block.AppendContext = (*appendContext)(nil)

func (c *appendContext) size() uint32 {
	return c.offset - headerBlockSize
}

func (c *appendContext) WriteOffset() uint32 {
	return c.offset
}

func (c *appendContext) Full() bool {
	return c.full != 0
}

func (c *appendContext) MarkFull() {
	c.full = 1
}

func (c *appendContext) FullEntry() block.Entry {
	return block.Entry{
		Offset: c.offset,
		Index:  c.num,
	}
}

// Make sure block implements block.TwoPCAppender.
var _ block.TwoPCAppender = (*Block)(nil)

func (b *Block) NewAppendContext(last *block.Entry) block.AppendContext {
	if last != nil {
		var full uint32
		if len(last.Payload) == 0 {
			full = 1
		}
		return &appendContext{
			offset: last.EndOffset(),
			num:    last.Index + 1,
			full:   full,
		}
	}

	// Copy append context.
	actx := b.actx
	return &actx
}

func (b *Block) PrepareAppend(
	ctx context.Context, appendCtx block.AppendContext, entries ...block.Entry,
) ([]block.Entry, error) {
	actx, _ := appendCtx.(*appendContext)

	var size uint32
	for i := range entries {
		entry := &entries[i]
		entry.Offset = actx.offset + size
		entry.Index = actx.num + uint32(i)
		size += uint32(entry.Size())
	}

	if !b.hasEnoughSpace(actx, size, len(entries)) {
		return nil, block.ErrNotEnoughSpace
	}

	actx.offset += size
	actx.num += uint32(len(entries))

	return entries, nil
}

func (b *Block) hasEnoughSpace(actx *appendContext, size uint32, num int) bool {
	return b.requireSpace(size, num) <= b.remaining(actx.size(), actx.num)
}

func (b *Block) requireSpace(size uint32, num int) uint32 {
	return size + indexSize*uint32(num) + block.EntryLengthSize
}

func (b *Block) CommitAppend(ctx context.Context, entries ...block.Entry) error {
	entries, err := b.trimEntries(ctx, entries)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	if err = b.checkEntries(ctx, entries); err != nil {
		return err
	}

	offset := entries[0].Offset
	last := &entries[len(entries)-1]
	size := last.EndOffset() - offset

	// Check free space.
	if !b.hasEnoughSpace(&b.actx, size, len(entries)) {
		actx := b.actx
		log.Error(ctx, "block: not enough space.", map[string]interface{}{
			"block_id":        b.id,
			"entry_size":      size,
			"entry_num":       len(entries),
			"append_context":  actx,
			"require_space":   b.requireSpace(size, len(entries)),
			"remaining_space": b.remaining(b.actx.size(), b.actx.num),
		})
		return block.ErrNotEnoughSpace
	}

	buf := make([]byte, size)
	indexes := make([]index, 0, len(entries))
	for _, entry := range entries {
		n, _ := entry.MarshalTo(buf[entry.Offset-offset:])
		indexes = append(indexes, index{
			offset: int64(entry.Offset),
			length: int32(n),
		})
	}

	n, err := b.f.WriteAt(buf, int64(offset))
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.indexes = append(b.indexes, indexes...)
	b.actx.num += uint32(len(entries))
	b.actx.offset += uint32(n)
	b.mu.Unlock()

	// if err = b.physicalFile.Sync(); err != nil {
	// 	return err
	// }

	return nil
}

func (b *Block) trimEntries(ctx context.Context, entries []block.Entry) ([]block.Entry, error) {
	num := b.actx.num
	for i := 0; i < len(entries); i++ {
		switch entry := &entries[i]; {
		case entry.Index < num:
			log.Warning(ctx, "block: entry index less than block num, skip this entry.", map[string]interface{}{
				"block_id": b.id,
				"index":    entry.Index,
				"num":      num,
			})
			continue
		case entry.Index > num:
			log.Error(ctx, "block: entry index greater than block num.", map[string]interface{}{
				"block_id": b.id,
				"index":    entry.Index,
				"num":      num,
			})
			return nil, errors.ErrInternal
		}
		if i != 0 {
			return entries[i:], nil
		}
		return entries, nil
	}
	return nil, nil
}

func (b *Block) checkEntries(ctx context.Context, entries []block.Entry) error {
	offset := entries[0].Offset
	if offset != b.actx.offset {
		log.Error(ctx, "block: entry offset is not equal than block wo.", map[string]interface{}{
			"block_id": b.id,
			"offset":   offset,
			"wo":       b.actx.offset,
			"index":    entries[0].Index,
		})
		return errors.ErrInternal
	}

	for i := 1; i < len(entries); i++ {
		entry := &entries[i]
		prev := &entries[i-1]
		if prev.Index+1 != entry.Index {
			log.Error(ctx, "block: entry index is discontinuous.", map[string]interface{}{
				"block_id": b.id,
				"index":    entry.Index,
				"prev":     prev.Index,
			})
			return errors.ErrInternal
		}
		if prev.EndOffset() != entry.Offset {
			log.Error(ctx, "block: entry offset is discontinuous.", map[string]interface{}{
				"block_id": b.id,
				"offset":   entry.Offset,
				"prev":     prev.Offset,
			})
			return errors.ErrInternal
		}
	}

	return nil
}

func (b *Block) MarkFull(ctx context.Context) error {
	b.mu.Lock()
	atomic.StoreUint32(&b.actx.full, 1)
	b.mu.Unlock()

	// TODO(james.yin): flush header and index
	if err := b.persistHeader(ctx); err != nil {
		return err
	}

	go func() {
		// FIXME(james.yin): wait complete when close.
		_ = b.persistIndex(ctx)
	}()

	return nil
}
