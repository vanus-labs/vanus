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

package wal

import (
	// standard libraries.
	"context"
	"io"
	"sync"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	blockSize = 32 * 1024
	fileSize  = 128 * 1024 * 1024
)

type blockWithSo struct {
	block
	// so is start offset
	so int64
}

type resultOrError struct {
	result []int64
	err    error
}

type entriesWithCallback struct {
	entries  [][]byte
	callback chan<- resultOrError
}

type blockWithArgs struct {
	block  *blockWithSo
	offset int
	own    bool
}

type callbackWithThreshold struct {
	callback  chan<- resultOrError
	result    []int64
	threshold int64
}

// WAL is write-ahead log.
type WAL struct {
	// pool of block
	pool sync.Pool

	// wb is the block currently being written to.
	wb *blockWithSo
	// wboff is the start offset of next wb used in allocateBlock()
	wboff int64

	stream *logStream

	appendc   chan entriesWithCallback
	callbackc chan callbackWithThreshold
	flushc    chan blockWithArgs
	weakupc   chan int64

	donec chan struct{}
	ctx   context.Context
}

func newBlock() interface{} {
	return &blockWithSo{
		block: block{
			buf: make([]byte, blockSize),
		},
	}
}

func newWAL(ctx context.Context, stream *logStream, pos int64) (*WAL, error) {
	w := &WAL{
		pool: sync.Pool{
			New: newBlock,
		},
		stream: stream,
		// TODO(james.yin): don't use magic numbers.
		appendc:   make(chan entriesWithCallback, 64),
		callbackc: make(chan callbackWithThreshold, 64),
		flushc:    make(chan blockWithArgs, 1024),
		weakupc:   make(chan int64, 1024),
		donec:     make(chan struct{}),
		ctx:       ctx,
	}

	w.wboff = pos
	w.wboff -= pos % blockSize

	w.wb = w.allocateBlock()

	// recover write block
	if pos > 0 {
		f := stream.selectFile(w.wb.so)
		if _, err := f.f.ReadAt(w.wb.buf, w.wb.so-f.so); err != nil {
			return nil, err
		}
		w.wb.wp = int(pos - w.wb.so)
		w.wb.fp = w.wb.wp
	}

	go w.runCallback()
	go w.runFlush()
	go w.runAppend()

	return w, nil
}

func (w *WAL) Wait() {
	<-w.donec
}

// Append appends entries to WAL. It blocks until all entries are persisted.
func (w *WAL) Append(entries [][]byte) ([]int64, error) {
	ch := make(chan resultOrError, 1)
	er := entriesWithCallback{
		entries:  entries,
		callback: ch,
	}
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case w.appendc <- er:
	}
	result := <-ch
	return result.result, result.err
}

func (w *WAL) runAppend() {
	period := 500 * time.Microsecond

	// create a stopped timer
	timer := time.NewTimer(period)
	if !timer.Stop() {
		<-timer.C
	}
	waiting := false

	for {
		select {
		case er := <-w.appendc:
			full, goahead := w.doAppend(er.entries, er.callback)
			if !full {
				if goahead {
					// reset timer
					if waiting && !timer.Stop() {
						// drain channel
						<-timer.C
					}
					timer.Reset(period)
					waiting = true
				} else if !waiting {
					// start timer
					timer.Reset(period)
					waiting = true
				}
			} else if waiting {
				// stop timer
				if !timer.Stop() {
					// drain channel
					<-timer.C
				}
				waiting = false
			}
		case <-timer.C:
			// timeout, flush
			w.flushc <- blockWithArgs{
				block: w.wb,
				// TODO(james.yin): Align to 4KB.
				offset: w.wb.Size(),
				own:    false,
			}
			waiting = false
		case <-w.ctx.Done():
			if waiting {
				timer.Stop()
			}
			// flush, then stop
			w.flushc <- blockWithArgs{
				block:  w.wb,
				offset: w.wb.Size(),
				own:    false,
			}
			close(w.flushc)
			return
		}
	}
}

// doAppend write entries to block(s). And return two flags: full and goahead.
// The full flag indicate last written block is full, and the
// goahead flag indicate switching to a new block.
func (w *WAL) doAppend(entries [][]byte, callback chan<- resultOrError) (bool, bool) {
	var full, goahead bool
	offsets := make([]int64, len(entries))
	for i, entry := range entries {
		records := record.Pack(entry, w.wb.Remaining(), blockSize)
		for j, record := range records {
			n, err := w.wb.Append(record)
			if err != nil {
				callback <- resultOrError{nil, err}
				return full, goahead
			}
			if j == len(records)-1 {
				offset := w.wb.so + int64(n)
				offsets[i] = offset
				if i == len(entries)-1 {
					// register callback
					w.callbackc <- callbackWithThreshold{
						callback:  callback,
						result:    offsets,
						threshold: offset,
					}
				}
			}
			if full = w.wb.full(n); full {
				// notify to flush
				w.flushc <- blockWithArgs{
					block:  w.wb,
					offset: w.wb.Capacity(),
					own:    true,
				}
				// switch wb
				w.wb = w.allocateBlock()
				goahead = true
			}
		}
	}
	return full, goahead
}

func (w *WAL) runFlush() {
	// TODO(james.yin): parallelizing, or batching
	for ba := range w.flushc {
		err := w.doFlush(ba.block, ba.offset)
		if err != nil {
			// TODO(james.yin): handle flush error
			panic(err)
		}
		if ba.own {
			w.freeBlock(ba.block)
		}
	}
	close(w.weakupc)
}

func (w *WAL) doFlush(fb *blockWithSo, offset int) error {
	writer := w.logWriter(fb.so)

	n, err := fb.Flush(writer, offset, fb.so)
	if err != nil {
		return err
	}

	// weakup
	w.weakupc <- fb.so + int64(n)

	return nil
}

func (w *WAL) runCallback() {
	var cb chan<- resultOrError
	var re []int64
	var th int64
	for offset := range w.weakupc {
		// NOTE: write cb to callbackc before writing offset to weakupc.
		if cb == nil {
			cb, re, th = w.nextCallback()
		}
		for cb != nil {
			if th > offset {
				break
			}
			cb <- resultOrError{
				result: re,
			}
			cb, re, th = w.nextCallback()
		}
	}
	close(w.donec)
}

func (w *WAL) nextCallback() (chan<- resultOrError, []int64, int64) {
	select {
	case c := <-w.callbackc:
		return c.callback, c.result, c.threshold
	default:
		return nil, nil, 0
	}
}

func (w *WAL) logWriter(offset int64) io.WriterAt {
	return w.stream.selectFile(offset)
}

func (w *WAL) allocateBlock() *blockWithSo {
	b, _ := w.pool.Get().(*blockWithSo)
	// reset block
	b.wp = 0
	b.fp = 0
	b.so = w.wboff
	w.wboff += blockSize
	return b
}

func (w *WAL) freeBlock(b *blockWithSo) {
	w.pool.Put(b)
}
