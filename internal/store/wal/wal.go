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

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/log"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io/engine"
	"github.com/linkall-labs/vanus/internal/store/io/stream"
	"github.com/linkall-labs/vanus/internal/store/io/zone/segmentedfile"
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

type AppendOneCallback = func(Range, error)

type AppendCallback = func([]Range, error)

// WAL is write-ahead log.
type WAL struct {
	sf *segmentedfile.SegmentedFile
	s  stream.Stream

	engine    engine.Interface
	scheduler stream.Scheduler

	blockSize int

	appendC chan *appender

	closeMu  sync.RWMutex
	appendWg sync.WaitGroup

	closeC chan struct{}
	doneC  chan struct{}
}

func Open(ctx context.Context, dir string, opts ...Option) (*WAL, error) {
	cfg := makeConfig(opts...)
	return open(ctx, dir, cfg)
}

func open(ctx context.Context, dir string, cfg config) (*WAL, error) {
	log.Info(ctx, "Open wal.", map[string]interface{}{
		"dir": dir,
		"pos": cfg.pos,
	})

	sf, err := segmentedfile.Open(dir, cfg.segmentedFileOptions()...)
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

	log.Info(ctx, "Checking wal is done.", map[string]interface{}{
		"dir": dir,
		"off": off,
	})

	scheduler := stream.NewScheduler(cfg.engine, cfg.streamSchedulerOptions()...)
	s := scheduler.Register(sf, off, true)

	w := &WAL{
		sf: sf,
		s:  s,

		engine:    cfg.engine,
		scheduler: scheduler,
		blockSize: cfg.blockSize,

		appendC: make(chan *appender, cfg.appendBufferSize),
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

func (w *WAL) AppendOne(ctx context.Context, entry []byte, cb AppendOneCallback) {
	w.append(ctx, [][]byte{entry}, false, func(rs []Range, err error) {
		if err != nil {
			cb(Range{}, err)
			return
		}

		cb(rs[0], nil)
	})
}

// Append appends entries to WAL.
func (w *WAL) Append(ctx context.Context, entries [][]byte, cb AppendCallback) {
	w.append(ctx, entries, false, cb)
}

func (w *WAL) append(ctx context.Context, entries [][]byte, direct bool, cb AppendCallback) {
	// Check entries.
	if len(entries) == 0 {
		cb(nil, nil)
	}

	// NOTE: Can not close the WAL while writing to appendC.
	w.closeMu.RLock()
	select {
	case <-w.closeC:
		// TODO(james.yin): invoke callback in another goroutine.
		cb(nil, ErrClosed)
	default:
		w.appendC <- w.newAppender(ctx, entries, direct, cb)
	}
	w.closeMu.RUnlock()
}

func (w *WAL) runAppend() {
	for task := range w.appendC {
		task.invoke()
	}

	w.appendWg.Wait()

	w.doClose()
}

func (w *WAL) Compact(ctx context.Context, off int64) error {
	return w.sf.Compact(off)
}

type appendResult struct {
	ranges []Range
	err    error
}

type appendFuture chan appendResult

func newAppendFuture() appendFuture {
	return make(appendFuture, 1)
}

func (af appendFuture) onAppended(ranges []Range, err error) {
	af <- appendResult{
		ranges: ranges,
		err:    err,
	}
}

func (af appendFuture) wait() ([]Range, error) {
	re := <-af
	return re.ranges, re.err
}

func Append(ctx context.Context, w *WAL, entries [][]byte) ([]Range, error) {
	future := newAppendFuture()
	w.append(ctx, entries, false, future.onAppended)
	return future.wait()
}

func DirectAppend(ctx context.Context, w *WAL, entries [][]byte) ([]Range, error) {
	future := newAppendFuture()
	w.append(ctx, entries, true, future.onAppended)
	return future.wait()
}

func AppendOne(ctx context.Context, w *WAL, entry []byte) (Range, error) {
	future := newAppendFuture()
	w.append(ctx, [][]byte{entry}, false, future.onAppended)
	rs, err := future.wait()
	if err != nil {
		return Range{}, err
	}
	return rs[0], nil
}

func DirectAppendOne(ctx context.Context, w *WAL, entry []byte) (Range, error) {
	future := newAppendFuture()
	w.append(ctx, [][]byte{entry}, true, future.onAppended)
	rs, err := future.wait()
	if err != nil {
		return Range{}, err
	}
	return rs[0], nil
}
