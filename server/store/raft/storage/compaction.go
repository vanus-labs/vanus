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

package storage

import (
	// standard libraries.
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	// first-party libraries.
	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/raft"
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/server/store/meta"
)

const (
	defaultCompactInterval = 30 * time.Second
)

var walCompactKey = []byte("wal/compact")

var ErrClosed = errors.New("WAL: closed")

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index greater than raftLog.applied.
func (s *Storage) Compact(ctx context.Context, i uint64) error {
	ci := s.compactedIndex()
	li := s.lastStableIndex()
	if i <= ci {
		log.Warn(ctx).
			Stringer("node_id", s.nodeID).
			Uint64("to_compact", i).
			Uint64("compacted_index", ci).
			Msg("raft log has been compacted")
		return raft.ErrCompacted
	}
	if i > li {
		log.Error(ctx).
			Stringer("node_id", s.nodeID).
			Uint64("to_compact", i).
			Uint64("last_index", li).
			Msg("compactedIndex is out of bound lastIndex")
		// FIXME(james.yin): error
		return raft.ErrCompacted
	}

	sz := i - ci
	remaining := s.length() - sz
	sr := s.stableLength() - sz

	ents := make([]raftpb.Entry, 1, 1+remaining)
	offs := make([]int64, 1, 1+sr)

	// Save compact information to dummy entry.
	ents[0].Index = s.ents[sz].Index
	ents[0].Term = s.ents[sz].Term

	// Copy remained entries.
	if remaining != 0 {
		ents = append(ents, s.ents[sz+1:]...)
		// NOTE: sr <= remaining
		if sr != 0 {
			offs = append(offs, s.offs[sz+1:]...)
			offs[0] = offs[1]
		}
	}

	// Compact WAL.
	_ = s.wal.tryCompact(ctx, s.nodeID, offs[0], s.offs[0], s.tail, ents[0].Index, ents[0].Term)

	// Reset log entries and offsets.
	s.mu.Lock()
	s.ents = ents
	s.mu.Unlock()
	s.offs = offs

	return nil
}

func (w *WAL) tryCompact(ctx context.Context, nodeID vanus.ID, offset, last, tail int64, index, term uint64) error {
	task := compactTask{
		nodeID: nodeID,
		offset: offset,
		last:   last,
		tail:   tail,
		info: compactInfo{
			index: index,
			term:  term,
		},
	}

	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	select {
	case <-w.closeC:
		return ErrClosed
	default:
	}

	select {
	case w.compactC <- task.compact:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WAL) markBarrier(ctx context.Context, nodeID vanus.ID, offset int64) error {
	task := compactTask{
		nodeID: nodeID,
		offset: offset,
	}

	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	select {
	case <-w.closeC:
		return ErrClosed
	default:
	}

	select {
	case w.compactC <- task.compact:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WAL) removeBarrier(ctx context.Context, nodeID vanus.ID, offset int64) error {
	task := compactTask{
		nodeID: nodeID,
		last:   offset,
	}

	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	select {
	case <-w.closeC:
		return ErrClosed
	default:
	}

	select {
	case w.compactC <- task.compact:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type compactTask struct {
	nodeID             vanus.ID
	offset, last, tail int64
	info               compactInfo
}

func (t *compactTask) compact(w *WAL, cCtx *compactContext) {
	// node is deleted.
	if !w.nodes[t.nodeID] {
		return
	}

	// Discard last barrier.
	if t.last != 0 {
		w.barrier.Remove(t.last)
	}
	// Set new barrier.
	if t.offset != 0 {
		w.barrier.Set(t.offset, t.nodeID)
	}
	if t.tail > cCtx.tail {
		cCtx.tail = t.tail
	}
	// Set compaction info.
	if !t.info.empty() {
		cCtx.infos[t.nodeID] = t.info
	}
}

func (w *WAL) addNode(ctx context.Context, nodeID vanus.ID) error {
	ch := make(chan error)
	task := adminTask{
		nodeID: nodeID,
		ch:     ch,
	}

	w.closeMu.RLock()
	select {
	case <-w.closeC:
	default:
	}

	select {
	case w.compactC <- task.addNode:
	case <-ctx.Done():
		w.closeMu.RUnlock()
		return ctx.Err()
	}
	w.closeMu.RUnlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WAL) removeNode(ctx context.Context, nodeID vanus.ID) error {
	ch := make(chan error)
	task := adminTask{
		nodeID: nodeID,
		ch:     ch,
	}

	w.closeMu.RLock()
	select {
	case <-w.closeC:
	default:
	}

	select {
	case w.compactC <- task.removeNode:
	case <-ctx.Done():
		w.closeMu.RUnlock()
		return ctx.Err()
	}
	w.closeMu.RUnlock()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WAL) recoverNode(nodeID vanus.ID, offset int64) {
	w.nodes[nodeID] = true
	if offset != 0 {
		w.barrier.Set(offset, nodeID)
	}
}

type adminTask struct {
	nodeID vanus.ID
	ch     chan<- error
}

var emptyCompact = make([]byte, 16)

func (t *adminTask) addNode(w *WAL, _ *compactContext) {
	w.nodes[t.nodeID] = true

	key := []byte(CompactKey(t.nodeID.Uint64()))
	w.stateStore.Store(context.Background(), key, emptyCompact, func(err error) {
		if err != nil {
			t.ch <- err
		}
		close(t.ch)
	})
}

func (t *adminTask) removeNode(w *WAL, cCtx *compactContext) {
	// Prevent compact on node.
	w.nodes[t.nodeID] = false
	delete(cCtx.infos, t.nodeID)

	w.stateStore.Delete(context.Background(), []byte(CompactKey(t.nodeID.Uint64())), func(err error) {
		if err != nil {
			// TODO(james.yin): handle error.
			panic(err)
		}

		close(t.ch)

		// Clean node to delete WAL.
		w.compactC <- (func(w *WAL, cc *compactContext) {
			delete(w.nodes, t.nodeID)
		})
	})
}

type compactInfo struct {
	index, term uint64
}

func (ci *compactInfo) empty() bool {
	return ci.index == 0
}

type logCompactInfos map[vanus.ID]compactInfo

var _ meta.Ranger = (logCompactInfos)(nil)

func (i logCompactInfos) Range(cb meta.RangeCallback) error {
	for id := range i {
		key := CompactKey(id.Uint64())
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
	tail      int64
	compacted int64
	toCompact int64
	infos     logCompactInfos
}

func loadCompactContext(stateStore *meta.SyncStore) *compactContext {
	cCtx := &compactContext{
		infos: make(logCompactInfos),
	}
	if v, ok := stateStore.Load(walCompactKey); ok {
		cCtx.compacted, _ = v.(int64)
	}
	cCtx.toCompact = cCtx.compacted
	return cCtx
}

func (c *compactContext) stale() bool {
	return c.toCompact > c.compacted || len(c.infos) != 0
}

func (c *compactContext) sync(ctx context.Context, stateStore *meta.SyncStore) bool {
	err := meta.BatchStore(ctx, stateStore, &compactMeta{
		infos:  c.infos,
		offset: c.toCompact,
	})
	if err != nil {
		log.Warn(ctx).Msg("sync compaction information failed")
		return false
	}
	c.compacted = c.toCompact
	c.infos = make(logCompactInfos)
	return true
}

func (w *WAL) runCompact() {
	ctx := context.Background()

	ticker := time.NewTicker(defaultCompactInterval)
	defer ticker.Stop()

	cCtx := loadCompactContext(w.stateStore)
	for {
		select {
		case task := <-w.compactC:
			task(w, cCtx)
		case <-ticker.C:
			w.doCompact(ctx, cCtx)
		case <-w.closeC:
			close(w.compactC)
			for task := range w.compactC {
				task(w, cCtx)
			}
			w.doCompact(ctx, cCtx)
			close(w.doneC)
			return
		}
	}
}

func (w *WAL) doCompact(ctx context.Context, cCtx *compactContext) {
	for {
		front := w.barrier.Front()

		//  No log entry in WAL.
		if front == nil {
			cCtx.toCompact = cCtx.tail
			break
		}

		if _, ok := w.nodes[front.Value.(vanus.ID)]; !ok {
			w.barrier.RemoveElement(front)
			continue
		}

		offset, _ := front.Key().(int64)
		cCtx.toCompact = offset
		break
	}

	if cCtx.stale() {
		log.Debug(ctx).
			Int64("offset", cCtx.toCompact).
			Msg("compact WAL of raft storage.")

		// Store compacted info and offset.
		if cCtx.sync(ctx, w.stateStore) {
			// Compact underlying WAL.
			_ = w.WAL.Compact(ctx, cCtx.compacted)
		}
	}
}

func CompactKey(id uint64) string {
	return fmt.Sprintf("block/%020d/compact", id)
}
