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

package block

import (
	// standard libraries.
	"errors"
	stdio "io"
	"os"
	"sync/atomic"
	"unsafe"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
)

var ErrAlreadyFlushed = errors.New("already flushed")

type flushTask struct {
	b      *Buffer
	writer io.WriterAt
	off    int
	cb     FlushCallback
	next   *flushTask
}

// Buffer is an append-only buffer for block IO.
//
// NOTE: calling Append and Flush is not thread-safe.
type Buffer struct {
	base int64
	buf  []byte
	// wp is write pointer
	wp int
	// fp is flush pointer
	fp int
	// cp is commit pointer
	cp int
	// nf is next flush task
	nf unsafe.Pointer
}

// Make sure Buffer implements Interface.
var _ Interface = (*Buffer)(nil)

func (b *Buffer) Base() int64 {
	return b.base
}

func (b *Buffer) Capacity() int {
	return len(b.buf)
}

func (b *Buffer) Size() int {
	return b.wp
}

func (b *Buffer) Committed() int {
	return b.cp
}

func (b *Buffer) Remaining() int {
	return b.remaining(b.Size())
}

func (b *Buffer) remaining(offset int) int {
	return b.Capacity() - offset
}

func (b *Buffer) Full() bool {
	return b.Remaining() == 0
}

func (b *Buffer) Empty() bool {
	return b.Size() == 0
}

func (b *Buffer) Append(r stdio.Reader) (int, error) {
	n, err := r.Read(b.buf[b.wp:])
	if err != nil && err != stdio.EOF { //nolint:errorlint // compare to EOF is ok.
		return 0, err
	}
	b.wp += n
	return b.wp, err
}

// Flush flushes data in the buffer to storage by writer.
// Invoking callbacks for multiple flushes on the same Buffer is sequence.
func (b *Buffer) Flush(writer io.WriterAt, cb FlushCallback) {
	// TODO(james.yin): Synchronization in concurrency.

	eo := b.wp

	// Already flush, skip.
	if b.fp >= eo {
		cb(b.fp, ErrAlreadyFlushed)
		return
	}

	so := b.fp
	b.fp = eo

	p := atomic.LoadPointer(&b.nf)

	// Shortcut if it is final flush.
	if eo == b.Capacity() && p == nil {
		writer.WriteAt(b.buf, b.base, so, eo, func(_ int, err error) {
			if err == nil {
				b.cp = eo
			}
			cb(eo, err)
		})
		return
	}

	task := &flushTask{
		b:      b,
		writer: writer,
		off:    eo,
		cb:     cb,
		next:   (*flushTask)(p),
	}

	for !atomic.CompareAndSwapPointer(&b.nf, p, unsafe.Pointer(task)) {
		p = atomic.LoadPointer(&b.nf)
		task.next = (*flushTask)(p)
	}

	if p != nil {
		return
	}

	// partial flush
	task.invoke(so)
}

func (ft *flushTask) invoke(so int) {
	b := ft.b
	ft.writer.WriteAt(b.buf, b.base, so, ft.off, ft.onWrite)
}

func (ft *flushTask) onWrite(_ int, err error) {
	b := ft.b
	offset := ft.off

	if err == nil {
		b.cp = offset
	}

	p, last := b.relocateFlushTask(ft)

	// NOTE: If it is final flush, DO NOT use b after invoke callback.
	ft.invokeCallback(err)

	if last == nil {
		if atomic.CompareAndSwapPointer(&b.nf, p, nil) {
			return
		}

		// reload
		p, last = b.relocateFlushTask(ft)
	}

	// truncate task list
	last.next = nil

	// TODO(james.yin): optimize goroutine
	go (*flushTask)(p).invoke(offset)
}

func (b *Buffer) relocateFlushTask(ft *flushTask) (unsafe.Pointer, *flushTask) {
	p := atomic.LoadPointer(&b.nf)

	var last *flushTask
	if t := (*flushTask)(p); t != ft {
		last = t
		for last.next != ft {
			last = last.next
		}
	}

	return p, last
}

func (ft *flushTask) invokeCallback(err error) {
	if ft.next != nil {
		ft.next.invokeCallback(err)
	}
	ft.cb(ft.off, err)
}

func (b *Buffer) RecoverFromFile(f *os.File, at int64, committed int) error {
	if _, err := f.ReadAt(b.buf, at); err != nil {
		return err
	}
	b.wp = committed
	b.fp = committed
	b.cp = committed
	return nil
}
