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
	"errors"
	"sync"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/block"
	"github.com/linkall-labs/vanus/internal/store/wal/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

var ErrClosed = errors.New("wal: closed")

type Result struct {
	Offsets []int64
	Err     error
}

func (re *Result) Offset() int64 {
	return re.Offsets[0]
}

type AppendCallback func(Result)

type appendTask struct {
	entries  [][]byte
	batching bool
	callback AppendCallback
}

type AppendOption func(*appendTask)

func WithoutBatching() AppendOption {
	return func(task *appendTask) {
		task.batching = false
	}
}

func WithCallback(callback AppendCallback) AppendOption {
	return func(task *appendTask) {
		task.callback = callback
	}
}

type flushTask struct {
	block  *block.Block
	offset int
	own    bool
}

type callbackTask struct {
	callback  AppendCallback
	offsets   []int64
	threshold int64
}

// WAL is write-ahead log.
type WAL struct {
	allocator *block.Allocator
	// wb is the block currently being written to.
	wb *block.Block

	stream *logStream
	engine io.Engine

	appendc   chan appendTask
	callbackc chan callbackTask
	flushc    chan flushTask
	weakupc   chan int64

	flushw sync.WaitGroup

	closec chan struct{}
	donec  chan struct{}
}

func NewWAL(dir string, opts ...Option) (*WAL, error) {
	cfg := makeConfig(dir, opts...)
	return newWAL(cfg)
}

func newWAL(cfg config) (*WAL, error) {
	blockSize := cfg.blockSize()
	pos := cfg.pos
	wbso := pos
	wbso -= pos % blockSize

	w := &WAL{
		allocator: block.NewAllocator(int(blockSize), wbso),
		stream:    cfg.stream,
		engine:    cfg.engine,
		appendc:   make(chan appendTask, cfg.appendBufferSize),
		callbackc: make(chan callbackTask, cfg.callbackBufferSize),
		flushc:    make(chan flushTask, cfg.flushBufferSize),
		weakupc:   make(chan int64, cfg.weakupBufferSize),
		closec:    make(chan struct{}),
		donec:     make(chan struct{}),
	}

	w.wb = w.allocator.Next()

	// Recover write block
	if pos > 0 {
		f := w.stream.selectFile(w.wb.SO, false)
		if f != nil {
			if err := f.Open(); err != nil {
				return nil, err
			}
			if err := w.wb.RecoverFromFile(f.f, w.wb.SO-f.so, int(pos-w.wb.SO)); err != nil {
				return nil, err
			}
		}
	}

	go w.runCallback()
	go w.runFlush()
	go w.runAppend(cfg.flushTimeout)

	return w, nil
}

func (w *WAL) Dir() string {
	return w.stream.dir
}

func (w *WAL) Close() {
	close(w.closec)
}

func (w *WAL) doClose() {
	w.engine.Close()
	close(w.donec)
}

func (w *WAL) Wait() {
	<-w.donec
}

type AppendOneFuture <-chan Result

func (f AppendOneFuture) Wait() (int64, error) {
	re := <-f
	if re.Err != nil {
		return -1, re.Err
	}
	return re.Offset(), nil
}

func (w *WAL) AppendOne(entry []byte, opts ...AppendOption) AppendOneFuture {
	return AppendOneFuture(w.Append([][]byte{entry}, opts...))
}

type AppendFuture <-chan Result

func (f AppendFuture) Wait() ([]int64, error) {
	re := <-f
	return re.Offsets, re.Err
}

// Append appends entries to WAL.
func (w *WAL) Append(entries [][]byte, opts ...AppendOption) AppendFuture {
	task := appendTask{
		entries:  entries,
		batching: true,
	}

	for _, opt := range opts {
		opt(&task)
	}

	var ch chan Result
	if task.callback == nil {
		ch = make(chan Result, 1)
		task.callback = func(re Result) {
			ch <- re
		}
	}

	select {
	case <-w.closec:
		// TODO(james.yin): invoke callback in another goroutine.
		task.callback(Result{
			Err: ErrClosed,
		})
	case w.appendc <- task:
	}

	return ch
}

func (w *WAL) runAppend(flushTimeout time.Duration) {
	// Create a stopped timer.
	timer := time.NewTimer(flushTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	waiting := false

	for {
		select {
		case task := <-w.appendc:
			full, goahead := w.doAppend(task.entries, task.callback)
			switch {
			case full || !task.batching:
				if !full {
					w.flushWritableBlock()
				}
				if waiting {
					// stop timer
					if !timer.Stop() {
						// drain channel
						<-timer.C
					}
					waiting = false
				}
			case goahead:
				// reset timer
				if waiting && !timer.Stop() {
					// drain channel
					<-timer.C
				}
				timer.Reset(flushTimeout)
				waiting = true
			case !waiting:
				// start timer
				timer.Reset(flushTimeout)
				waiting = true
			}
		case <-timer.C:
			// timeout, flush
			w.flushWritableBlock()
			waiting = false
		case <-w.closec:
			if waiting {
				timer.Stop()
			}
			// flush, then stop
			w.flushWritableBlock()
			close(w.flushc)
			return
		}
	}
}

func (w *WAL) flushWritableBlock() {
	w.flushc <- flushTask{
		block:  w.wb,
		offset: w.wb.Size(),
		own:    false,
	}
}

// doAppend write entries to block(s). And return two flags: full and goahead.
// The full flag indicate last written block is full, and the
// goahead flag indicate switching to a new block.
func (w *WAL) doAppend(entries [][]byte, callback AppendCallback) (bool, bool) {
	var full, goahead bool
	offsets := make([]int64, len(entries))
	for i, entry := range entries {
		records := record.Pack(entry, w.wb.Remaining(), w.allocator.BlockSize())
		for j, record := range records {
			n, err := w.wb.Append(record)
			if err != nil {
				callback(Result{nil, err})
				return full, goahead
			}
			if j == len(records)-1 {
				offset := w.wb.SO + int64(n)
				offsets[i] = offset
				if i == len(entries)-1 {
					// register callback
					w.callbackc <- callbackTask{
						callback:  callback,
						offsets:   offsets,
						threshold: offset,
					}
				}
			}
			if full = w.wb.FullWithOff(n); full {
				// notify to flush
				w.flushc <- flushTask{
					block:  w.wb,
					offset: w.wb.Capacity(),
					own:    true,
				}
				// switch wb
				w.wb = w.allocator.Next()
				goahead = true
			}
		}
	}
	return full, goahead
}

func (w *WAL) runFlush() {
	for task := range w.flushc {
		// Copy
		fb := task.block
		own := task.own

		writer := w.logWriter(fb.SO)

		w.flushw.Add(1)
		fb.Flush(writer, task.offset, fb.SO, func(off int64, err error) {
			if err != nil {
				panic(err)
			}

			// Weakup callbacks.
			w.weakupc <- fb.SO + off

			w.flushw.Done()

			if own {
				w.allocator.Free(fb)
			}
		})
	}

	// Wait in-flight flush tasks.
	w.flushw.Wait()

	close(w.weakupc)
}

func (w *WAL) logWriter(offset int64) io.WriterAt {
	f := w.stream.selectFile(offset, true)
	return io.WriteAtFunc(func(b []byte, off int64, cb io.WriteCallback) {
		f.WriteAt(w.engine, b, off, cb)
	})
}

func (w *WAL) runCallback() {
	var task *callbackTask
	for offset := range w.weakupc {
		// NOTE: write cb to callbackc before writing offset to weakupc.
		if task == nil {
			task = w.nextCallbackTask()
		}
		for task != nil {
			if task.threshold > offset {
				break
			}
			task.callback(Result{
				Offsets: task.offsets,
			})
			task = w.nextCallbackTask()
		}
	}

	// Weakup in-flight append tasks.
	w.weakupPendingTasks(task)

	w.doClose()
}

func (w *WAL) weakupPendingTasks(task *callbackTask) {
	if task != nil {
		task.callback(Result{
			Err: ErrClosed,
		})
	}
	for {
		task = w.nextCallbackTask()
		if task == nil {
			break
		}
		task.callback(Result{
			Err: ErrClosed,
		})
	}
	for {
		select {
		case at := <-w.appendc:
			at.callback(Result{
				Err: ErrClosed,
			})
			continue
		default:
		}
		break
	}
}

func (w *WAL) nextCallbackTask() *callbackTask {
	select {
	case c := <-w.callbackc:
		return &c
	default:
		return nil
	}
}
