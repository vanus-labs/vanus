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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	// first-party project.
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/observability/tracing"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/block"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
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
	ctx      context.Context
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
	ctx    context.Context
	block  *block.Block
	offset int
	own    bool
}

type callbackTask struct {
	ctx       context.Context
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

	appendC   chan appendTask
	callbackC chan callbackTask
	flushC    chan flushTask
	wakeupC   chan int64

	closeMu sync.RWMutex
	flushWg sync.WaitGroup

	closeC chan struct{}
	doneC  chan struct{}

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

	// Start offset of writable block.
	so := off
	so -= off % cfg.blockSize

	w := &WAL{
		allocator: block.NewAllocator(int(cfg.blockSize), so),
		stream:    stream,
		engine:    cfg.engine,
		appendC:   make(chan appendTask, cfg.appendBufferSize),
		callbackC: make(chan callbackTask, cfg.callbackBufferSize),
		flushC:    make(chan flushTask, cfg.flushBufferSize),
		wakeupC:   make(chan int64, cfg.wakeupBufferSize),
		closeC:    make(chan struct{}),
		doneC:     make(chan struct{}),
		tracer:    tracing.NewTracer("store.wal.walog", trace.SpanKindInternal),
	}

	w.wb = w.allocator.Next()

	// Recover write block
	if off > 0 {
		f := w.stream.selectFile(ctx, w.wb.SO, false)
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
	w.closeMu.Lock()
	defer w.closeMu.Unlock()

	select {
	case <-w.closeC:
	default:
		close(w.closeC)
	}
}

func (w *WAL) doClose() {
	ctx, span := w.tracer.Start(context.Background(), "doClose")
	defer span.End()

	w.engine.Close()
	w.stream.Close(ctx)
	close(w.doneC)
}

func (w *WAL) Wait() {
	<-w.doneC
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
		ctx:      ctx,
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

	// NOTE: Can not close the WAL while writing to appendC.
	w.closeMu.RLock()
	select {
	case <-w.closeC:
		// TODO(james.yin): invoke callback in another goroutine.
		task.callback(Result{
			Err: ErrClosed,
		})
	default:
		w.appendC <- task
	}
	w.closeMu.RUnlock()

	return ch
}

func (w *WAL) runAppend(flushTimeout time.Duration) {
	// Create flush timer.
	timer := time.NewTimer(flushTimeout)
	running, waiting := true, false
	var start time.Time

	aCtx := context.Background()
	for {
		select {
		case task := <-w.appendC:
			aCtx = task.ctx
			full, goahead := w.doAppend(aCtx, task.entries, task.callback)
			switch {
			case full || !task.batching:
				if !full {
					w.flushWritableBlock(aCtx, false)
				}
				// stop timer
				trace.SpanFromContext(aCtx).AddEvent("Discard flush timer")
				waiting = false
			case goahead || !waiting:
				// reset timer
				waiting = true
				start = time.Now()
				if !running {
					trace.SpanFromContext(aCtx).AddEvent("Start flush timer")
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
			w.flushWritableBlock(aCtx, true)
			waiting = false
			running = false
		case <-w.closeC:
			if running {
				timer.Stop()
			}
			// flush, then stop
			if waiting {
				w.flushWritableBlock(aCtx, false)
			}
			close(w.flushC)
			return
		}
	}
}

func (w *WAL) flushWritableBlock(ctx context.Context, timeout bool) {
	span := trace.SpanFromContext(ctx)

	span.AddEvent("Publishing flush task", trace.WithAttributes(attribute.Bool("timeout", timeout)))
	w.flushC <- flushTask{
		ctx:    ctx,
		block:  w.wb,
		offset: w.wb.Size(),
		own:    false,
	}
	span.AddEvent("Notified flush task")
}

// doAppend writes entries to block(s). And return two flags: full and goahead.
// The full flag indicate last written block is full, and the
// goahead flag indicate switching to a new block.
func (w *WAL) doAppend(ctx context.Context, entries [][]byte, callback AppendCallback) (bool, bool) {
	_, span := w.tracer.Start(ctx, "doAppend")
	defer span.End()

	var entrySize, recordCount, recordSize int

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
					w.callbackC <- callbackTask{
						ctx:       ctx,
						callback:  callback,
						ranges:    ranges,
						threshold: offset,
					}
				}
			}
			if full = w.wb.FullWithOffset(n); full {
				// notify to flush
				span.AddEvent("Publishing flush task",
					trace.WithAttributes(attribute.Bool("timeout", false)))
				w.flushC <- flushTask{
					ctx:    ctx,
					block:  w.wb,
					offset: w.wb.Capacity(),
					own:    true,
				}
				span.AddEvent("Notified flush task")

				// switch wb
				w.wb = w.allocator.Next()
				goahead = true
			}

			recordSize += record.Size()
		}

		recordCount += len(records)
		entrySize += len(entry)
	}

	metrics.WALEntryWriteCounter.Add(float64(len(entries)))
	metrics.WALEntryWriteSizeCounter.Add(float64(entrySize))
	metrics.WALRecordWriteCounter.Add(float64(recordCount))
	metrics.WALRecordWriteSizeCounter.Add(float64(recordSize))

	span.SetAttributes(
		attribute.Int("entry_count", len(entries)),
		attribute.Int("entry_size", entrySize),
		attribute.Int("record_count", recordCount),
		attribute.Int("record_size", recordSize))

	return full, goahead
}

func (w *WAL) runFlush() {
	for task := range w.flushC {
		ctx, span := w.tracer.Start(task.ctx, "doFlush")

		// Copy
		fb := task.block
		own := task.own

		writer := w.logWriter(ctx, fb.SO)

		w.flushWg.Add(1)
		fb.Flush(writer, task.offset, fb.SO, func(off int64, err error) {
			span.End()

			if err != nil {
				panic(err)
			}

			// Wakeup callbacks.
			w.wakeupC <- fb.SO + off

			w.flushWg.Done()

			if own {
				w.allocator.Free(fb)
			}
		})
	}

	// Wait in-flight flush tasks.
	w.flushWg.Wait()

	close(w.wakeupC)
}

func (w *WAL) logWriter(ctx context.Context, offset int64) io.WriterAt {
	f := w.stream.selectFile(ctx, offset, true)

	return io.WriteAtFunc(func(b []byte, off int64, so, eo int, cb io.WriteCallback) {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int("real_so", so),
			attribute.Int("real_eo", eo),
			attribute.Int("real_size", eo-so),
			attribute.Int("block_size", len(b)),
			attribute.Bool("block_full", eo == 0 || eo == len(b)))

		f.WriteAt(w.engine, b, off, so, eo, cb)
	})
}

func (w *WAL) runCallback() {
	var task *callbackTask
	for offset := range w.wakeupC {
		// NOTE: write cb to callbackC before writing offset to wakeupC.
		if task == nil {
			task = w.nextCallbackTask()
		}
		for task != nil {
			if task.threshold > offset {
				break
			}

			_, span := w.tracer.Start(task.ctx, "doCallback")
			task.callback(Result{
				Ranges: task.ranges,
			})
			span.End()

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
		case at := <-w.appendC:
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
	case c := <-w.callbackC:
		return &c
	default:
		return nil
	}
}

func (w *WAL) Compact(ctx context.Context, off int64) error {
	return w.stream.compact(ctx, off)
}
