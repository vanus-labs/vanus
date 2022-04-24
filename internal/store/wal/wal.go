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
)

type blockWithSo struct {
	block
	// so is start offset
	so int64
}

type entriesWithResult struct {
	entries [][]byte
	result  chan<- error
}

type blockWithArgs struct {
	block  *blockWithSo
	offset int
	own    bool
}

type callbackWithOffset struct {
	callback chan<- error
	offset   int64
}

// WAL is write-ahead log.
type WAL struct {
	pool      sync.Pool
	wb        *blockWithSo
	wboff     int64
	appendc   chan entriesWithResult
	callbackc chan callbackWithOffset
	flushc    chan blockWithArgs
	weakupc   chan int64
	donec     chan struct{}
	ctx       context.Context
}

func NewWAL(ctx context.Context) *WAL {
	// TODO(james.yin): log file
	w := &WAL{
		pool: sync.Pool{
			New: func() interface{} {
				return &blockWithSo{
					block: block{
						buf: make([]byte, blockSize),
					},
				}
			},
		},
		appendc:   make(chan entriesWithResult, 64),
		callbackc: make(chan callbackWithOffset, 64),
		flushc:    make(chan blockWithArgs, 1024),
		weakupc:   make(chan int64, 1024),
		donec:     make(chan struct{}),
		ctx:       ctx,
	}
	w.wb = w.allocateBlock()
	go w.runCallback()
	go w.runFlush()
	go w.runAppend()
	return w
}

func (w *WAL) Wait() {
	<-w.donec
}

// Append appends entries to WAL. It blocks until all entries are persisted.
func (w *WAL) Append(entries [][]byte) error {
	ch := make(chan error, 1)
	er := entriesWithResult{
		entries: entries,
		result:  ch,
	}
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	case w.appendc <- er:
	}
	return <-ch
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
			full, goahead := w.doAppend(er.entries, er.result)
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
			} else {
				if waiting {
					// stop timer
					if !timer.Stop() {
						// drain channel
						<-timer.C
					}
					waiting = false
				}
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
				offset: w.wb.Capacity(),
				own:    true,
			}
			close(w.flushc)
			return
		}
	}
}

// doAppend write entries to block(s). And return two flags: full and goahead.
// The full flag indicate last written block is full, and the
// goahead flag indicate switching to a new block.
func (w *WAL) doAppend(entries [][]byte, callback chan<- error) (bool, bool) {
	var full, goahead bool
	for i, entry := range entries {
		records := record.Pack(entry, w.wb.Remaining(), blockSize)
		for j, record := range records {
			n, err := w.wb.Append(record)
			if err != nil {
				callback <- err
				return full, goahead
			}
			if i == len(entries)-1 && j == len(records)-1 {
				// register callback
				w.callbackc <- callbackWithOffset{
					callback: callback,
					offset:   w.wb.so + int64(n),
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
	// TODO(james.yin): parallelizing
	for ba := range w.flushc {
		err := w.doFlush(ba.block, ba.offset)
		if err != nil {
			// TODO(james.yin): handle flush error
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
	var cb chan<- error
	var cboff int64
	for offset := range w.weakupc {
		if cb == nil {
			cb, cboff = w.nextCallback()
		}
		for cb != nil {
			if cboff > offset {
				break
			}
			close(cb)
			cb, cboff = w.nextCallback()
		}
	}
	close(w.donec)
}

func (w *WAL) nextCallback() (chan<- error, int64) {
	select {
	case co := <-w.callbackc:
		return co.callback, co.offset
	default:
		return nil, 0
	}
}

func (w *WAL) logWriter(offset int64) io.WriterAt {
	// TODO(james.yin): log file writer
	return dummyWriter
}

func (w *WAL) allocateBlock() *blockWithSo {
	b := w.pool.Get().(*blockWithSo)
	// reset block
	b.wp = 0
	b.fp = 0
	// FIXME(james.yin): set b.so
	b.so = w.wboff
	w.wboff += blockSize
	return b
}

func (w *WAL) freeBlock(b *blockWithSo) {
	w.pool.Put(b)
}
