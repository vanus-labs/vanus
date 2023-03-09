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
	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/executor"
	"github.com/vanus-labs/vanus/internal/store/io"
	"github.com/vanus-labs/vanus/internal/store/io/block"
	"github.com/vanus-labs/vanus/internal/store/io/engine"
	"github.com/vanus-labs/vanus/internal/store/io/zone"
)

type Scheduler interface {
	Close()

	Register(z zone.Interface, wo int64, direct bool) Stream
	Unregister(s Stream)
}

type scheduler struct {
	e  engine.Interface
	bp *block.BufferPool
	pq pendingQueue

	callbackExecutor executor.MultiFlow
}

// Make sure scheduler implements Scheduler.
var _ Scheduler = (*scheduler)(nil)

func NewScheduler(e engine.Interface, opts ...Option) Scheduler {
	cfg := makeConfig(opts...)
	return new(scheduler).init(e, cfg)
}

func (s *scheduler) init(e engine.Interface, cfg config) *scheduler {
	s.e = e
	s.bp = block.NewBufferPool(cfg.flushBatchSize)
	s.pq.init(cfg.flushDelayTime)
	s.callbackExecutor.Init(cfg.callbackParallel, false, true)
	return s
}

func (s *scheduler) Close() {
	s.pq.Close()
	s.e.Close()
	s.callbackExecutor.Close()
}

func (s *scheduler) Register(z zone.Interface, wo int64, direct bool) Stream {
	so := wo % int64(s.bp.BufferSize())
	base := wo - so

	var buf *block.Buffer
	if so != 0 {
		buf = s.getBuffer(base)
		f, off := z.Raw(base)
		if f == nil {
			// TODO(james.yin)
			panic("invalid zone")
		}
		if err := buf.RecoverFromFile(f, off, int(so), direct); err != nil {
			panic(err)
		}
	}

	ss := &stream{
		s:                s,
		z:                z,
		buf:              buf,
		off:              base,
		pending:          make(map[int64]*flushTask, 4),
		callbackExecutor: s.callbackExecutor.NewFlow(),
	}
	ss.pending[base] = &flushTask{
		ready: true,
	}

	return ss
}

func (s *scheduler) Unregister(ss Stream) {
	sss, _ := ss.(*stream)
	sss.mu.Lock()
	sss.cancelFlushTimer()
	sss.mu.Unlock()
	sss.callbackExecutor.Close()
}

func (s *scheduler) writeAt(z zone.Interface, b []byte, off int64, so, eo int, cb io.WriteCallback) {
	s.e.WriteAt(z, b, off, so, eo, cb)
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
