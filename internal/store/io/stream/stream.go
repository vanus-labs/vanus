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
	"sync"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/io/block"
)

type Stream interface {
	File() *os.File
	WriteOffset() int64

	Append(b []byte, cb io.WriteCallback)
}

type flushTask struct {
	ready bool
	off   int
	cbs   []io.WriteCallback
}

type stream struct {
	s *scheduler
	f *os.File

	mu  sync.Mutex
	buf *block.Buffer
	// off is the base offset of Buffer buf.
	off int64
	// dirty is a flag to indicate whether the Buffer buf is dirty.
	dirty   bool
	waiting []io.WriteCallback

	timer PendingID

	pending sync.Map
}

// Make sure handle implements Stream and io.WriterAt.
var (
	_ Stream      = (*stream)(nil)
	_ io.WriterAt = (*stream)(nil)
	_ PendingTask = (*stream)(nil)
)

func (s *stream) File() *os.File {
	return s.f
}

func (s *stream) WriteOffset() int64 {
	if s.buf == nil {
		return s.off
	}
	return s.off + int64(s.buf.Size())
}

func (s *stream) Append(b []byte, cb io.WriteCallback) {
	flushBatchSize := s.s.bufferSize()

	s.mu.Lock()
	defer s.mu.Unlock()

	in := &input{data: b}

	if buf := s.buf; buf != nil {
		_, _ = buf.Append(in)

		eof := in.eof()
		if eof {
			s.waiting = append(s.waiting, cb)
		}

		if buf.Full() {
			// Flush block directly.

			if s.dirty {
				s.dirty = false
				s.cancelFlushTimer()
			}

			s.flushBuffer(buf, s.waiting)
			s.off += int64(flushBatchSize)
			s.buf = nil
			s.waiting = nil
		} else if !s.dirty {
			s.dirty = true
			s.startFlushTimer()
		}

		if eof {
			return
		}
	}

	for {
		switch remaining := in.remaining(); {
		case remaining > flushBatchSize:
			ob := block.Oneshot(s.off, in.advance(flushBatchSize))
			s.flushBlock(ob, nil)
			s.off += int64(flushBatchSize)
		case remaining == flushBatchSize:
			ob := block.Oneshot(s.off, in.advance(flushBatchSize))
			s.flushBlock(ob, []io.WriteCallback{cb})
			s.off += int64(flushBatchSize)
			return
		default:
			buf := s.s.getBuffer(s.off)
			_, _ = buf.Append(in)

			s.buf = buf
			s.waiting = []io.WriteCallback{cb}
			s.dirty = true

			s.startFlushTimer()
			return
		}
	}
}

func (s *stream) startFlushTimer() {
	if s.timer != nil {
		return
	}
	s.timer = s.s.delayFlush(s)
}

func (s *stream) cancelFlushTimer() {
	if s.timer == nil {
		return
	}
	s.s.cancelFlushTask(s.timer)
	s.timer = nil
}

func (s *stream) OnTimeout(pid PendingID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timer == pid {
		s.dirty = false
		s.flushBlock(s.buf, s.waiting)
		s.waiting = nil
		s.timer = nil
	}
}

func (s *stream) flushBuffer(b *block.Buffer, cbs []io.WriteCallback) {
	b.Flush(s, func(off int, err error) {
		base := b.Base()
		s.s.putBuffer(b)
		if err != nil && err != block.ErrAlreadyFlushed { //nolint:errorlint // compare to ErrAlreadyFlushed is ok
			panic(err)
		}
		s.onFlushed(base, off, cbs)
	})
}

func (s *stream) flushBlock(b block.Interface, cbs []io.WriteCallback) {
	base := b.Base()
	b.Flush(s, func(off int, err error) {
		if err != nil && err != block.ErrAlreadyFlushed { //nolint:errorlint // compare to ErrAlreadyFlushed is ok
			panic(err)
		}
		s.onFlushed(base, off, cbs)
	})
}

func (s *stream) onFlushed(base int64, off int, cbs []io.WriteCallback) {
	var empty bool
	v, loaded := s.pending.LoadAndDelete(base)

	// Wait previous block flushed.
	if !loaded {
		_, loaded = s.pending.LoadOrStore(base, &flushTask{
			off: off,
			cbs: cbs,
		})
		if !loaded {
			return
		}
	} else {
		ft, _ := v.(*flushTask)
		if !ft.ready {
			ft.off = off
			ft.cbs = append(ft.cbs, cbs...)
			// Write back
			_, loaded = s.pending.LoadOrStore(base, ft)
			if !loaded {
				return
			}
			cbs = ft.cbs
		} else {
			empty = true
		}
	}

	// FIXME(james.yin): pass n
	invokeCallbacks(cbs, 0, nil)

	flushBatchSize := s.s.bufferSize()

	// Partial flush.
	if off != flushBatchSize {
		if empty {
			s.pending.Store(base, &flushTask{
				ready: true,
			})
		}
		return
	}

	if !empty {
		s.pending.Delete(base)
	}

	for {
		// Check next block.
		base += int64(flushBatchSize)

		for {
			_, loaded = s.pending.LoadOrStore(base, &flushTask{
				ready: true,
			})
			if !loaded {
				return
			}

			v, _ = s.pending.LoadAndDelete(base)
			ft, _ := v.(*flushTask)

			// FIXME(james.yin): pass n
			invokeCallbacks(ft.cbs, 0, nil)

			if ft.off == flushBatchSize {
				break
			}
		}
	}
}

func invokeCallbacks(cbs []io.WriteCallback, n int, err error) {
	if len(cbs) == 0 {
		return
	}

	for _, cb := range cbs {
		cb(n, err)
	}
}

func (s *stream) WriteAt(b []byte, off int64, so, eo int, cb io.WriteCallback) {
	s.s.writeAt(s.f, b, off, so, eo, cb)
}
