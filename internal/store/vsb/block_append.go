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
	"bytes"
	"context"
	stderr "errors"
	stdio "io"
	"sync/atomic"
	"time"

	// third-party libraries.
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/log"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/io"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

var (
	errCorruptedFragment = stderr.New("vsb: corrupted fragment")
	dummyReader          = stdio.LimitReader(nil, 0)
)

type appendContext struct {
	seq      int64
	offset   int64
	archived uint32
}

// Make sure appendContext implements block.AppendContext.
var _ block.AppendContext = (*appendContext)(nil)

func (c *appendContext) size(dataOffset int64) int64 {
	return c.offset - dataOffset
}

func (c *appendContext) WriteOffset() int64 {
	return c.offset
}

func (c *appendContext) Archived() bool {
	return c.archived != 0
}

// Make sure vsBlock implements block.TwoPCAppender.
var _ block.TwoPCAppender = (*vsBlock)(nil)

func (b *vsBlock) NewAppendContext(last block.Fragment) block.AppendContext {
	if last != nil {
		_, entry, _ := b.dec.UnmarshalLast(last.Payload())
		seq := ceschema.SequenceNumber(entry)
		actx := &appendContext{
			seq:    seq + 1,
			offset: last.EndOffset(),
		}
		if ceschema.EntryType(entry) == ceschema.End {
			actx.archived = 1
		}
		return actx
	}

	// Copy append context.
	actx := b.actx
	return &actx
}

func (b *vsBlock) PrepareAppend(
	ctx context.Context, appendCtx block.AppendContext, entries ...block.Entry,
) ([]int64, block.Fragment, bool, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.vsb.vsBlock.PrepareAppend() Start")
	defer span.AddEvent("store.vsb.vsBlock.PrepareAppend() End")

	actx, _ := appendCtx.(*appendContext)

	num := int64(len(entries))
	ents := make([]block.Entry, num)
	seqs := make([]int64, num)

	// TODO(james.yin): fill auto fields in a general way.
	now := time.Now().UnixMilli()
	for i := int64(0); i < num; i++ {
		seq := actx.seq + i
		ents[i] = wrapEntry(entries[i], ceschema.CloudEvent, seq, now)
		seqs[i] = seq
	}

	frag := newFragment(actx.offset, ents, b.enc)

	actx.offset += int64(frag.Size())
	actx.seq += num

	full := actx.size(b.dataOffset) >= b.capacity
	if full && b.lis != nil {
		m, indexes := makeSnapshot(b.actx, b.indexes)
		b.lis.OnArchived(b.stat(m, indexes, block.StateArchiving))
	}

	return seqs, frag, full, nil
}

func (b *vsBlock) PrepareArchive(ctx context.Context, appendCtx block.AppendContext) (block.Fragment, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.vsb.vsBlock.PrepareArchive() Start")
	defer span.AddEvent("store.vsb.vsBlock.PrepareArchive() End")

	actx, _ := appendCtx.(*appendContext)

	end := wrapEntry(&block.EmptyEntryExt{}, ceschema.End, actx.seq, time.Now().UnixMilli())
	frag := newFragment(actx.offset, []block.Entry{end}, b.enc)

	actx.offset += int64(frag.Size())
	actx.seq++
	actx.archived = 1

	return frag, nil
}

func (b *vsBlock) CommitAppend(ctx context.Context, frag block.Fragment, cb block.CommitAppendCallback) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.vsb.vsBlock.CommitAppend() Start")
	defer span.AddEvent("store.vsb.vsBlock.CommitAppend() End")

	if frag == nil {
		b.s.Append(dummyReader, func(n int, err error) {
			cb()
		})
		return
	}

	// TODO(james.yin): get offset from Stream or AppendContext?
	off := b.s.WriteOffset() // b.actx.offset
	if frag.EndOffset() <= off {
		log.Info(ctx, "vsb: data of fragment has been written, skip this entry.", map[string]interface{}{
			"block_id":              b.id,
			"expected":              off,
			"fragment_start_offset": frag.StartOffset(),
			"fragment_end_offset":   frag.EndOffset(),
		})
		// TODO(james.yin): invoke callback.
		return
	}
	if frag.StartOffset() != off {
		log.Error(ctx, "vsb: missing some fragments.", map[string]interface{}{
			"block_id": b.id,
			"expected": off,
			"found":    frag.StartOffset(),
		})
		// TODO(james.yin): invoke callback.
		return
	}

	indexes, seq, archived, _ := b.buildIndexes(ctx, b.actx.seq, frag)

	b.actx.seq = seq
	b.actx.offset = frag.EndOffset()

	if !archived {
		b.s.Append(bytes.NewReader(frag.Payload()), func(n int, err error) {
			log.Debug(context.Background(), "acquiring index write lock", map[string]interface{}{
				"block_id": b.id,
			})
			b.mu.Lock()
			b.indexes = append(b.indexes, indexes...)
			log.Info(context.Background(), "release index write lock", map[string]interface{}{
				"block_id": b.id,
			})
			b.mu.Unlock()

			cb()
		})
		return
	}

	atomic.StoreUint32(&b.actx.archived, 1)

	b.wg.Add(1)
	b.s.Append(bytes.NewReader(frag.Payload()), func(n int, err error) {
		if len(indexes) != 0 {
			log.Debug(context.Background(), "acquiring index write lock", map[string]interface{}{
				"block_id": b.id,
			})
			b.mu.Lock()
			b.indexes = append(b.indexes, indexes...)
			log.Info(context.Background(), "release index write lock", map[string]interface{}{
				"block_id": b.id,
			})
			b.mu.Unlock()
		}

		cb()

		m, i := makeSnapshot(b.actx, b.indexes)

		go b.appendIndexEntry(ctx, i, func(n int, err error) {
			defer b.wg.Done()
			b.indexOffset = m.writeOffset
			b.indexLength = n
			_ = b.persistHeader(ctx, m)
		})

		if b.lis != nil {
			b.lis.OnArchived(b.stat(m, i, block.StateArchived))
		}
	})
}

func (b *vsBlock) buildIndexes(
	ctx context.Context, expected int64, frag block.Fragment,
) ([]index.Index, int64, bool, error) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.vsb.vsBlock.buildIndexes() Start")
	defer span.AddEvent("store.vsb.vsBlock.buildIndexes() End")

	base := frag.StartOffset()
	data := frag.Payload()

	var indexes []index.Index
	for off, sz := 0, len(data); off < sz; {
		n, entry, _ := b.dec.Unmarshal(data[off:])
		if seq := ceschema.SequenceNumber(entry); seq != expected {
			return nil, 0, false, errCorruptedFragment
		}
		expected++

		if ceschema.EntryType(entry) == ceschema.End {
			// End entry must be the last.
			if off+n != sz {
				return nil, 0, false, errCorruptedFragment
			}
			return indexes, expected, true, nil
		}

		idx := index.NewIndex(base+int64(off), int32(n), index.WithEntry(entry))
		indexes = append(indexes, idx)

		off += n
	}

	return indexes, expected, false, nil
}

func (b *vsBlock) appendIndexEntry(ctx context.Context, indexes []index.Index, cb io.WriteCallback) {
	entry := index.NewEntry(indexes)
	sz := b.enc.Size(entry)
	data := make([]byte, sz)
	if _, err := b.enc.MarshalTo(ctx, entry, data); err != nil {
		cb(0, err)
		return
	}

	b.s.Append(bytes.NewReader(data), cb)
	// return sz, nil
}
