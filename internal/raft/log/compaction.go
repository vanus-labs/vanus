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

package log

import (
	// standard libraries.
	"context"
	"encoding/binary"
	"fmt"
	"time"

	// third-party libraries.
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultCompactInterval = 30 * time.Second
)

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (l *Log) Compact(ctx context.Context, i uint64) error {
	ctx, span := l.tracer.Start(ctx, "Compact",
		trace.WithAttributes(attribute.Int64("to_compact", int64(i))))
	defer span.End()

	span.AddEvent("Acquiring lock")
	l.Lock()
	span.AddEvent("Got lock")

	defer l.Unlock()

	ci := l.compactedIndex()
	li := l.lastStableIndex()
	span.SetAttributes(
		attribute.Int64("compacted_index", int64(ci)),
		attribute.Int64("last_index", int64(li)),
	)
	if i <= ci {
		log.Warning(ctx, "raft log has been compacted", map[string]interface{}{
			"block_id":        l.nodeID,
			"to_compact":      i,
			"compacted_index": ci,
		})
		return raft.ErrCompacted
	}
	if i > li {
		log.Error(ctx, "compactedIndex is out of bound lastIndex", map[string]interface{}{
			"block_id":   l.nodeID,
			"to_compact": i,
			"last_index": li,
		})
		// FIXME(james.yin): error
		return raft.ErrCompacted
	}

	sz := i - ci
	remaining := l.length() - sz
	sr := l.stableLength() - sz
	span.SetAttributes(attribute.Int64("remaining", int64(remaining)),
		attribute.Int64("stable_remaining", int64(sr)))

	span.AddEvent("Allocating cache")
	ents := make([]raftpb.Entry, 1, 1+remaining)
	offs := make([]int64, 1, 1+sr)

	// Save compact information to dummy entry.
	ents[0].Index = l.ents[sz].Index
	ents[0].Term = l.ents[sz].Term

	// Copy remained entries.
	if remaining != 0 {
		span.AddEvent("Coping remained entries")
		ents = append(ents, l.ents[sz+1:]...)
		// NOTE: sr <= remaining
		if sr != 0 {
			offs = append(offs, l.offs[sz+1:]...)
			offs[0] = offs[1]
		}
		span.AddEvent("Copied remained entries")
	}

	// Compact WAL.
	l.wal.tryCompact(ctx, offs[0], l.offs[0], l.nodeID, ents[0].Index, ents[0].Term)

	// Reset log entries and offsets.
	l.ents = ents
	l.offs = offs

	return nil
}

func (w *WAL) reserve(cb reserveCallback) error {
	w.compactMu.RLock()
	defer w.compactMu.RUnlock()
	off, old, err := cb()
	if err != nil {
		return err
	}
	w.compactC <- compactTask{
		offset: off,
		last:   old,
	}
	return nil
}

func (w *WAL) tryCompact(ctx context.Context, offset, last int64, nodeID vanus.ID, index, term uint64) {
	_, span := w.tracer.Start(ctx, "tryCompact")
	defer span.End()

	w.updateC <- compactTask{
		offset: offset,
		last:   last,
		nodeID: nodeID,
		info: compactInfo{
			index: index,
			term:  term,
		},
	}
}

type logCompactInfos map[vanus.ID]compactInfo

var _ meta.Ranger = (logCompactInfos)(nil)

func (i logCompactInfos) Range(cb meta.RangeCallback) error {
	for id := range i {
		key := fmt.Sprintf("block/%020d/compact", id.Uint64())
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(value[0:8], i[id].index)
		binary.BigEndian.PutUint64(value[8:16], i[id].term)
		if err := cb([]byte(key), value); err != nil {
			return err
		}
	}
	return nil
}

type compactMeta struct {
	infos  logCompactInfos
	offset int64
}

var _ meta.Ranger = (*compactMeta)(nil)

func (m *compactMeta) Range(cb meta.RangeCallback) error {
	if err := m.infos.Range(cb); err != nil {
		return err
	}
	if m.offset != 0 {
		if err := cb(walCompactKey, m.offset); err != nil {
			return err
		}
	}
	return nil
}

type compactContext struct {
	compacted int64
	toCompact int64
	infos     logCompactInfos
	metaStore *meta.SyncStore
}

func loadCompactContext(metaStore *meta.SyncStore) *compactContext {
	cCtx := &compactContext{
		infos:     make(logCompactInfos),
		metaStore: metaStore,
	}
	if v, ok := metaStore.Load(walCompactKey); ok {
		cCtx.compacted, _ = v.(int64)
	}
	cCtx.toCompact = cCtx.compacted
	return cCtx
}

func (c *compactContext) stale() bool {
	return c.toCompact > c.compacted || len(c.infos) != 0
}

func (c *compactContext) sync(ctx context.Context) {
	c.metaStore.BatchStore(ctx, &compactMeta{
		infos:  c.infos,
		offset: c.toCompact,
	})
	c.compacted = c.toCompact
	c.infos = make(logCompactInfos)
}

var emptyMark = struct{}{}

func (w *WAL) runBarrierUpdate() {
	for task := range w.updateC {
		w.compactMu.Lock()
		w.compactC <- task
		w.compactMu.Unlock()
	}

	close(w.compactC)
}

func (w *WAL) runCompact() {
	ctx := context.Background()

	ticker := time.NewTicker(defaultCompactInterval)
	defer ticker.Stop()

	cCtx := loadCompactContext(w.metaStore)
	for {
		select {
		case task, ok := <-w.compactC:
			_, span := w.tracer.Start(ctx, "runCompactTask", trace.WithAttributes(
				attribute.Bool("timeout", false),
				attribute.Bool("closed", !ok),
			))

			if !ok {
				for task := range w.compactC {
					w.compact(cCtx, task)
				}
				w.doCompact(ctx, cCtx)
				close(w.doneC)
				span.End()
				return
			}

			w.compact(cCtx, task)

			span.End()
		case <-ticker.C:
			w.doCompact(ctx, cCtx)
		}
	}
}

func (w *WAL) compact(cCtx *compactContext, compact compactTask) {
	// Discard last barrier.
	if compact.last != 0 {
		w.barrier.Remove(compact.last)
	}
	// Set new barrier.
	if compact.offset != 0 {
		w.barrier.Set(compact.offset, emptyMark)
	}
	// Set compaction info.
	if compact.nodeID != 0 {
		cCtx.infos[compact.nodeID] = compact.info
	}
	if front := w.barrier.Front(); front != nil {
		offset, _ := front.Key().(int64)
		cCtx.toCompact = offset
	}
	// TODO(james.yin): no log entry in WAL.
}

func (w *WAL) doCompact(ctx context.Context, cCtx *compactContext) {
	ctx, span := w.tracer.Start(ctx, "doCompact")
	defer span.End()
	if cCtx.stale() {
		log.Debug(ctx, "compact WAL of raft log.", map[string]interface{}{
			"offset": cCtx.toCompact,
		})
		// Store compacted info and offset.
		cCtx.sync(ctx)
		// Compact underlying WAL.
		_ = w.WAL.Compact(ctx, cCtx.compacted)
	}
}
