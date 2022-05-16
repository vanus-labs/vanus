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
	"github.com/linkall-labs/raft"
	"github.com/linkall-labs/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	"github.com/linkall-labs/vanus/observability/log"
)

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (l *Log) Compact(i uint64) error {
	l.Lock()
	defer l.Unlock()

	ci := l.compactedIndex()
	if i <= ci {
		log.Warning(context.Background(), "raft log has been compacted", map[string]interface{}{})
		return raft.ErrCompacted
	}
	if i > l.lastIndex() {
		log.Error(context.Background(), "conpactedIndex is out of bound lastIndex", map[string]interface{}{
			"compactedIndex": i,
			"lastIndex":      l.lastIndex(),
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
	last := l.offs[0]

	// Copy remained entries.
	if sz < l.length() {
		ents = append(ents, l.ents[sz+1:]...)
		offs = append(offs, l.offs[sz+1:]...)
	}

	// Reset log entries.
	l.ents = ents
	l.offs = offs

	// Compact WAL.
	var compact int64
	if remaining != 0 {
		compact = offs[1]
	}
	offs[0] = compact
	l.wal.tryCompact(compact, last, l.nodeID, l.ents[0].Index, l.ents[0].Term)

	return nil
}

func (w *WAL) suppressCompact(cb exeCallback) error {
	result := make(chan error, 1)
	w.exec <- exeTask{
		cb:     cb,
		result: result,
	}
	return <-result
}

func (w *WAL) tryCompact(offset, last int64, nodeID vanus.ID, index, term uint64) {
	w.exec <- exeTask{
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
	value := make([]byte, 16)
	for id := range i {
		key := fmt.Sprintf("block/%020d/compact", id.Uint64())
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

var emptyMark = struct{}{}

func (w WAL) run() {
	for task := range w.exec {
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
}

func (w *WAL) runCompact() {
	peroid := 30 * time.Second
	ticker := time.NewTicker(peroid)
	defer ticker.Stop()

	var compacted int64
	if v, ok := w.metaStore.Load(walCompactKey); ok {
		compacted, _ = v.(int64)
	}

	toCompact := compacted
	infos := make(logCompactInfos)
	for {
		select {
		case compact := <-w.compactc:
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
				infos[compact.nodeID] = compact.info
			}
			if front := w.barrier.Front(); front != nil {
				offset, _ := front.Key().(int64)
				toCompact = offset
			}
			// TODO(james.yin): no log entry in WAL.
		case <-ticker.C:
			if toCompact > compacted || len(infos) != 0 {
				log.Debug(context.TODO(), "compact WAL of raft log.", map[string]interface{}{
					"offset": toCompact,
				})
				w.metaStore.BatchStore(&compactMeta{
					infos:  infos,
					offset: toCompact,
				})
				compacted = toCompact
				infos = make(logCompactInfos)
			}
		}
	}
}
