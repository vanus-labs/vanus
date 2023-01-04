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

	// third-party project.
	"go.opentelemetry.io/otel/trace"

	// first-party project.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io/engine"
	"github.com/linkall-labs/vanus/internal/store/io/stream"
	"github.com/linkall-labs/vanus/internal/store/io/zone/segmentedfile"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	logFileExt = ".log"
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

type AppendCallback = func([]Range, error)

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

// WAL is write-ahead log.
type WAL struct {
	sf *segmentedfile.SegmentedFile
	s  stream.Stream

	engine    engine.Interface
	scheduler stream.Scheduler

	blockSize int

	appendC chan appendTask

	closeMu  sync.RWMutex
	appendWg sync.WaitGroup

	closeC chan struct{}
	doneC  chan struct{}
}

func Open(ctx context.Context, dir string, opts ...Option) (*WAL, error) {
	ctx, span := tracing.Start(ctx, "store.wal", "Open")
	defer span.End()
	cfg := makeConfig(opts...)
	return open(ctx, dir, cfg)
}

func open(ctx context.Context, dir string, cfg config) (*WAL, error) {
	log.Info(ctx, "Open wal.", map[string]interface{}{
		"dir": dir,
		"pos": cfg.pos,
	})

	sf, err := segmentedfile.Open(dir, segmentedfile.WithExtension(logFileExt),
		segmentedfile.WithSegmentSize(cfg.fileSize))
	if err != nil {
		return nil, err
	}

	// Check wal entries from pos.
	off, err := scanLogEntries(sf, cfg.blockSize, cfg.pos, cfg.cb)
	if err != nil {
		return nil, err
	}

	// Skip padding.
	if padding := int64(cfg.blockSize) - off%int64(cfg.blockSize); padding < record.HeaderSize {
		off += padding
	}

	scheduler := stream.NewScheduler(cfg.engine, cfg.blockSize, cfg.flushTimeout)
	s := scheduler.Register(sf, off)

	w := &WAL{
		sf: sf,
		s:  s,

		engine:    cfg.engine,
		scheduler: scheduler,
		blockSize: cfg.blockSize,

		appendC: make(chan appendTask, cfg.appendBufferSize),
		closeC:  make(chan struct{}),
		doneC:   make(chan struct{}),
	}

	go w.runAppend()

	return w, nil
}

func (w *WAL) Dir() string {
	return w.sf.Dir()
}

func (w *WAL) Close() {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()

	select {
	case <-w.closeC:
	default:
		close(w.closeC)
		close(w.appendC)
	}
}

func (w *WAL) doClose() {
	w.engine.Close()
	w.sf.Close()
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
	span := trace.SpanFromContext(ctx)
	span.AddEvent("store.wal.WAL.Append() Start")
	defer span.AddEvent("store.wal.WAL.Append() End")

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
		task.callback = func(ranges []Range, err error) {
			ch <- Result{
				Ranges: ranges,
				Err:    err,
			}
		}
	}

	// Check entries.
	if len(entries) == 0 {
		task.callback(nil, nil)
	}

	// NOTE: Can not close the WAL while writing to appendC.
	w.closeMu.RLock()
	select {
	case <-w.closeC:
		// TODO(james.yin): invoke callback in another goroutine.
		task.callback(nil, ErrClosed)
	default:
		w.appendC <- task
	}
	w.closeMu.RUnlock()

	return ch
}

func (w *WAL) runAppend() {
	for task := range w.appendC {
		w.newAppender(task.ctx, task.entries, task.callback).invoke()
	}

	w.appendWg.Wait()

	w.doClose()
}

func (w *WAL) Compact(ctx context.Context, off int64) error {
	return w.sf.Compact(off)
}
