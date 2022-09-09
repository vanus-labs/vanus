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
	"errors"
	"sync"
	"time"

	// third-party project.
	oteltracer "go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/block"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
	"github.com/linkall-labs/vanus/observability/tracing"
)

var (
	ErrClosed          = errors.New("wal: closed")
	ErrNotFoundLogFile = errors.New("wal: not found log file")
)

type Range struct {
	SO int64
	EO int64
}

type Result struct {
	Ranges []Range
	Err    error
}

func (re *Result) Range() Range {
	return re.Ranges[0]
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
	ranges    []Range
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
	wakeupc   chan int64

	closemu sync.RWMutex
	flushwg sync.WaitGroup

	closec chan struct{}
	donec  chan struct{}

	tracer *tracing.Tracer
}

func Open(ctx context.Context, dir string, opts ...Option) (*WAL, error) {
	ctx, span := tracing.Start(ctx, "store.wal", "Open")
	defer span.End()
	cfg := makeConfig(opts...)
	return open(ctx, dir, cfg)
}

func open(ctx context.Context, dir string, cfg config) (*WAL, error) {
	stream, err := recoverLogStream(ctx, dir, cfg)
	if err != nil {
		return nil, err
	}

	// Check wal entries from pos.
	off, err := stream.Range(ctx, cfg.pos, cfg.cb)
	if err != nil {
		return nil, err
	}

	wbso := off
	wbso -= off % cfg.blockSize

	w := &WAL{
		allocator: block.NewAllocator(int(cfg.blockSize), wbso),
		stream:    stream,
		engine:    cfg.engine,
		appendc:   make(chan appendTask, cfg.appendBufferSize),
		callbackc: make(chan callbackTask, cfg.callbackBufferSize),
		flushc:    make(chan flushTask, cfg.flushBufferSize),
		wakeupc:   make(chan int64, cfg.wakeupBufferSize),
		closec:    make(chan struct{}),
		donec:     make(chan struct{}),
		tracer:    tracing.NewTracer("store.wal.walog", oteltracer.SpanKindInternal),
	}

	w.wb = w.allocator.Next()

	// Recover write block
	if off > 0 {
		f := w.stream.selectFile(w.wb.SO, false)
		if f == nil {
			return nil, ErrNotFoundLogFile
		}
		if err := f.Open(false); err != nil {
			return nil, err
		}
		if err := w.wb.RecoverFromFile(f.f, w.wb.SO-f.so, int(off-w.wb.SO)); err != nil {
			return nil, err
		}
	}

	go w.runCallback() //nolint:contextcheck // wrong advice
	go w.runFlush()
	go w.runAppend(cfg.flushTimeout)

	return w, nil
}

func (w *WAL) Dir() string {
	return w.stream.dir
}

func (w *WAL) Close() {
	w.closemu.Lock()
	defer w.closemu.Unlock()

	select {
	case <-w.closec:
	default:
		close(w.closec)
	}
}

func (w *WAL) doClose() {
	ctx, span := w.tracer.Start(context.Background(), "doClose")
	defer span.End()

	w.engine.Close()
	w.stream.Close(ctx)
	close(w.donec)
}

func (w *WAL) Wait() {
	<-w.donec
}

type AppendOneFuture <-chan Result

func (f AppendOneFuture) Wait() (Range, error) {
	re := <-f
	if re.Err != nil {
		return Range{}, re.Err
	}
	return re.Range(), nil
}

func (w *WAL) AppendOne(ctx context.Context, entry []byte, opts ...AppendOption) AppendOneFuture {
	return AppendOneFuture(w.Append(ctx, [][]byte{entry}, opts...))
}

type AppendFuture <-chan Result

func (f AppendFuture) Wait() ([]Range, error) {
	re := <-f
	return re.Ranges, re.Err
}

// Append appends entries to WAL.
func (w *WAL) Append(ctx context.Context, entries [][]byte, opts ...AppendOption) AppendFuture {
	_, span := w.tracer.Start(ctx, "Append")
	defer span.End()

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

	// Check entries.
	if len(entries) == 0 {
		task.callback(Result{})
	}

	// NOTE: Can not close the WAL while writing to appendc.
	w.closemu.RLock()
	select {
	case <-w.closec:
		// TODO(james.yin): invoke callback in another goroutine.
		task.callback(Result{
			Err: ErrClosed,
		})
	default:
		w.appendc <- task
	}
	w.closemu.RUnlock()

	return ch
}

func (w *WAL) runAppend(flushTimeout time.Duration) {
	// Create flush timer.
	timer := time.NewTimer(flushTimeout)
	running := true
	waiting := false
	var start time.Time

	for {
		select {
		case task := <-w.appendc:
			full, goahead := w.doAppend(task.entries, task.callback)
			switch {
			case full || !task.batching:
				if !full {
					w.flushWritableBlock()
				}
				// stop timer
				waiting = false
			case goahead || !waiting:
				// reset timer
				waiting = true
				start = time.Now()
				if !running {
					timer.Reset(flushTimeout)
					running = true
				}
			}
		case <-timer.C:
			// timer stopped
			if !waiting {
				running = false
				break
			}

			d := time.Since(start)
			if d < flushTimeout {
				timer.Reset(flushTimeout - d)
				break
			}

			// timeout, flush
			w.flushWritableBlock()
			waiting = false
			running = false
		case <-w.closec:
			if running {
				timer.Stop()
			}
			// flush, then stop
			if waiting {
				w.flushWritableBlock()
			}
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
	ranges := make([]Range, len(entries))
	for i, entry := range entries {
		ranges[i].SO = w.wb.WriteOffset()
		records := record.Pack(entry, w.wb.Remaining(), w.allocator.BlockSize())
		for j, record := range records {
			n, err := w.wb.Append(record)
			if err != nil {
				callback(Result{nil, err})
				return full, goahead
			}
			if j == len(records)-1 {
				offset := w.wb.SO + int64(n)
				ranges[i].EO = offset
				if i == len(entries)-1 {
					// register callback
					w.callbackc <- callbackTask{
						callback:  callback,
						ranges:    ranges,
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

		w.flushwg.Add(1)
		fb.Flush(writer, task.offset, fb.SO, func(off int64, err error) {
			if err != nil {
				panic(err)
			}

			// Wakeup callbacks.
			w.wakeupc <- fb.SO + off

			w.flushwg.Done()

			if own {
				w.allocator.Free(fb)
			}
		})
	}

	// Wait in-flight flush tasks.
	w.flushwg.Wait()

	close(w.wakeupc)
}

func (w *WAL) logWriter(offset int64) io.WriterAt {
	f := w.stream.selectFile(offset, true)
	return io.WriteAtFunc(func(b []byte, off int64, cb io.WriteCallback) {
		f.WriteAt(w.engine, b, off, cb)
	})
}

func (w *WAL) runCallback() {
	var task *callbackTask
	for offset := range w.wakeupc {
		// NOTE: write cb to callbackc before writing offset to wakeupc.
		if task == nil {
			task = w.nextCallbackTask()
		}
		for task != nil {
			if task.threshold > offset {
				break
			}
			task.callback(Result{
				Ranges: task.ranges,
			})
			task = w.nextCallbackTask()
		}
	}

	// Wakeup in-flight append tasks.
	w.wakeupPendingTasks(task)

	w.doClose()
}

func (w *WAL) wakeupPendingTasks(task *callbackTask) {
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

func (w *WAL) Compact(off int64) error {
	return w.stream.compact(off)
}
