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

package stream

import (
	// standard libraries.
	"os"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/io/block"
	"github.com/linkall-labs/vanus/internal/store/io/engine"
)

type Scheduler interface {
	Close()

	Register(f *os.File, off int64) Stream
	Unregister(s Stream)
}

type scheduler struct {
	e  engine.Interface
	bp *block.BufferPool
	pq *pendingQueue
}

// Make sure scheduler implements Scheduler.
var _ Scheduler = (*scheduler)(nil)

func NewScheduler(e engine.Interface, flushBatchSize int, flushDelayTime time.Duration) Scheduler {
	bp := block.NewBufferPool(flushBatchSize)
	pq := newPendingQueue(flushDelayTime)
	s := &scheduler{
		e:  e,
		bp: bp,
		pq: pq,
	}
	return s
}

func (s *scheduler) Close() {
	s.pq.Close()
	s.e.Close()
}

func (s *scheduler) Register(f *os.File, off int64) Stream {
	so := off % int64(s.bp.BufferSize())
	base := off - so

	var buf *block.Buffer
	if so != 0 {
		buf = s.getBuffer(base)
		if err := buf.RecoverFromFile(f, base, int(so)); err != nil {
			panic(err)
		}
	}

	ss := &stream{
		s:   s,
		f:   f,
		buf: buf,
		off: base,
	}
	ss.pending.Store(base, &flushTask{
		ready: true,
	})

	return ss
}

func (s *scheduler) Unregister(ss Stream) {
}

func (s *scheduler) writeAt(f *os.File, b []byte, off int64, so, eo int, cb io.WriteCallback) {
	s.e.WriteAt(f, b, off, so, eo, cb)
}

func (s *scheduler) bufferSize() int {
	return s.bp.BufferSize()
}

func (s *scheduler) getBuffer(base int64) *block.Buffer {
	return s.bp.Get(base)
}

func (s *scheduler) putBuffer(b *block.Buffer) {
	s.bp.Put(b)
}

func (s *scheduler) delayFlush(ss *stream) PendingID {
	return s.pq.Push(ss)
}

func (s *scheduler) cancelFlushTask(pid PendingID) {
	s.pq.Cancel(pid)
}
