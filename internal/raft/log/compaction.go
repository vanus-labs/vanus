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
	l.Lock()
	defer l.Unlock()

	ci := l.compactedIndex()
	if i <= ci {
		log.Warning(context.Background(), "raft log has been compacted", map[string]interface{}{})
		return raft.ErrCompacted
	}
	if i > l.lastIndex() {
		log.Error(context.Background(), "conpactedIndex is out of bound lastIndex", map[string]interface{}{
			"compacted_index": i,
			"last_index":      l.lastIndex(),
		})
		// FIXME(james.yin): error
		return raft.ErrCompacted
	}

	sz := i - ci
	remaining := l.length() - sz
	ents := make([]raftpb.Entry, 1, 1+remaining)
	offs := make([]int64, 1, 1+remaining)

	// Save compact information to dummy entry.
	ents[0].Index = l.ents[sz].Index
	ents[0].Term = l.ents[sz].Term

	// Copy remained entries.
	if remaining != 0 {
		ents = append(ents, l.ents[sz+1:]...)
		offs = append(offs, l.offs[sz+1:]...)
		offs[0] = offs[1]
	}

	// Compact WAL.
	l.wal.tryCompact(ctx, offs[0], l.offs[0], l.nodeID, ents[0].Index, ents[0].Term)

	// Reset log entries and offsets.
	l.ents = ents
	l.offs = offs

	return nil
}

func (w *WAL) suppressCompact(cb executeCallback) error {
	result := make(chan error, 1)
	w.executec <- executeTask{
		cb:     cb,
		result: result,
	}
	return <-result
}

func (w *WAL) tryCompact(ctx context.Context, offset, last int64, nodeID vanus.ID, index, term uint64) {
	w.executec <- executeTask{
		cb: func() (compactTask, error) {
			return compactTask{
				offset: offset,
				last:   last,
				nodeID: nodeID,
				info: compactInfo{
					index: index,
					term:  term,
				},
			}, nil
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
	cctx := &compactContext{
		infos:     make(logCompactInfos),
		metaStore: metaStore,
	}
	if v, ok := metaStore.Load(walCompactKey); ok {
		cctx.compacted, _ = v.(int64)
	}
	cctx.toCompact = cctx.compacted
	return cctx
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

func (w *WAL) run() {
	for task := range w.executec {
		ct, err := task.cb()
		if task.result != nil {
			if err != nil {
				task.result <- err
				continue
			}
			w.compactc <- ct
			close(task.result)
		} else if err == nil {
			w.compactc <- ct
		}
	}
	close(w.compactc)
}

func (w *WAL) runCompact() {
	ticker := time.NewTicker(defaultCompactInterval)
	defer ticker.Stop()

	cctx := loadCompactContext(w.metaStore)
	for {
		select {
		case task, ok := <-w.compactc:
			if !ok {
				for task := range w.compactc {
					w.compact(cctx, task)
				}
				w.doCompact(cctx)
				close(w.donec)
				return
			}
			w.compact(cctx, task)
		case <-ticker.C:
			w.doCompact(cctx)
		}
	}
}

func (w *WAL) compact(cctx *compactContext, compact compactTask) {
	// Discard last barrier.
	if compact.last != 0 {
		w.barrier.Remove(compact.last)
	}
	// Set new barrier.
	if compact.offset != 0 {
		w.barrier.Set(compact.offset, emptyMark)
	}
	// Set compation info.
	if compact.nodeID != 0 {
		cctx.infos[compact.nodeID] = compact.info
	}
	if front := w.barrier.Front(); front != nil {
		offset, _ := front.Key().(int64)
		cctx.toCompact = offset
	}
	// TODO(james.yin): no log entry in WAL.
}

func (w *WAL) doCompact(cctx *compactContext) {
	ctx, span := w.tracer.Start(context.Background(), "doCompact")
	defer span.End()
	if cctx.stale() {
		log.Debug(context.TODO(), "compact WAL of raft log.", map[string]interface{}{
			"offset": cctx.toCompact,
		})
		// Store compacted info and offset.
		cctx.sync(ctx)
		// Compact underlying WAL.
		_ = w.WAL.Compact(cctx.compacted)
	}
}
