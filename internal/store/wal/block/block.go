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
	"os"
	"sync/atomic"
	"unsafe"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

type FlushCallback func(off int64, err error)

type flushTask struct {
	writer io.WriterAt
	offset int
	cb     FlushCallback
	next   *flushTask
}

func (ft *flushTask) invokeCallback(err error) {
	if ft.next != nil {
		ft.next.invokeCallback(err)
	}
	ft.cb(int64(ft.offset), err)
}

type block struct {
	buf []byte
	// wp is write pointer
	wp int
	// fp is flush pointer
	fp int
	// cp is commit pointer
	cp int
	// nf is next flush task
	nf unsafe.Pointer
}

func (b *block) Capacity() int {
	return len(b.buf)
}

func (b *block) Size() int {
	return b.wp
}

func (b *block) Committed() int {
	return b.cp
}

func (b *block) Remaining() int {
	return b.remaining(b.Size())
}

func (b *block) remaining(offset int) int {
	return b.Capacity() - offset
}

func (b *block) Full() bool {
	return b.Remaining() < record.HeaderSize
}

func (b *block) FullWithOffset(off int) bool {
	return b.remaining(off) < record.HeaderSize
}

func (b *block) Append(r record.Record) (int, error) {
	n, err := r.MarshalTo(b.buf[b.wp:])
	if err != nil {
		return 0, err
	}
	b.wp += n
	return b.wp, nil
}

func (b *block) Flush(writer io.WriterAt, offset int, base int64, cb FlushCallback) {
	// Already flushed, skip.
	if b.fp >= offset {
		cb(int64(b.fp), nil)
		return
	}

	fp := b.fp
	b.fp = offset

	task := &flushTask{
		writer: writer,
		offset: offset,
		cb:     cb,
	}

	p := atomic.LoadPointer(&b.nf)
	task.next = (*flushTask)(p)
	for !atomic.CompareAndSwapPointer(&b.nf, p, unsafe.Pointer(task)) {
		p = atomic.LoadPointer(&b.nf)
		task.next = (*flushTask)(p)
	}

	if p != nil {
		return
	}

	b.doFlush(task, base, fp)
}

func (b *block) doFlush(task *flushTask, base int64, so int) {
	offset := task.offset
	task.writer.WriteAt(b.buf, base, so, offset, func(_ int, err error) {
		if err == nil {
			b.cp = offset
		}

		p := atomic.LoadPointer(&b.nf)
		ft := (*flushTask)(p)

		var last *flushTask
		for ft.offset != offset {
			last = ft
			ft = ft.next
		}

		ft.invokeCallback(err)

		if last == nil {
			if atomic.CompareAndSwapPointer(&b.nf, p, nil) {
				return
			}

			// reload
			p = atomic.LoadPointer(&b.nf)
			last = (*flushTask)(p)
			for last.next.offset != offset {
				last = last.next
			}
		}

		// truncate task list
		last.next = nil

		go b.doFlush((*flushTask)(p), base, offset)
	})
}

func (b *block) RecoverFromFile(f *os.File, at int64, committed int) error {
	if _, err := f.ReadAt(b.buf, at); err != nil {
		return err
	}
	b.wp = committed
	b.fp = committed
	b.cp = committed
	return nil
}

type Block struct {
	block
	// SO is start offset
	SO int64
}

func (b *Block) WriteOffset() int64 {
	return b.SO + int64(b.Size())
}
