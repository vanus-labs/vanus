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
	"bytes"
	"context"
	"errors"
	"os"
	"sort"
	"sync"

	// third-party libraries.
	"github.com/ncw/directio"

	// first-party libraries.
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
	errutil "github.com/linkall-labs/vanus/pkg/util/errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

var (
	ErrOutOfRange = errors.New("WAL: out of range")
	errEndOfLog   = errors.New("WAL: end of log")
)

type OnEntryCallback func(entry []byte, r Range) error

type logStream struct {
	stream    []*logFile
	dir       string
	blockSize int64
	fileSize  int64
	mu        sync.RWMutex
	tracer    *tracing.Tracer
}

func (s *logStream) Close(ctx context.Context) {
	for _, f := range s.stream {
		if err := f.Close(); err != nil {
			log.Error(ctx, "Close log file failed.", map[string]interface{}{
				"path":       f.path,
				log.KeyError: err,
			})
		}
	}
}

func (s *logStream) firstFile() *logFile {
	return s.stream[0]
}

func (s *logStream) lastFile() *logFile {
	return s.stream[len(s.stream)-1]
}

func (s *logStream) selectFile(offset int64, autoCreate bool) *logFile {
	s.mu.RLock()

	sz := len(s.stream)
	if sz == 0 {
		s.mu.RUnlock()

		if offset == 0 {
			if !autoCreate {
				return nil
			}
			return s.createNextFile(nil)
		}
		panic("log stream not begin from 0")
	}

	// Fast return for append.
	if last := s.lastFile(); offset >= last.so {
		s.mu.RUnlock()

		if offset < last.eo {
			return last
		}
		if offset == last.eo {
			if !autoCreate {
				return nil
			}
			return s.createNextFile(last)
		}
		panic("log stream overflow")
	}

	defer s.mu.RUnlock()

	first := s.firstFile()
	if offset < first.so {
		panic("log stream underflow")
	}

	i := sort.Search(sz-1, func(i int) bool {
		return s.stream[i].eo > offset
	})
	if i < len(s.stream)-1 {
		return s.stream[i]
	}

	panic("unreachable")
}

func (s *logStream) createNextFile(last *logFile) *logFile {
	var off int64
	if last != nil {
		off = last.eo
	}

	next, err := createLogFile(s.dir, off, s.fileSize, true)
	if err != nil {
		panic(err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stream = append(s.stream, next)

	return next
}

type scanContext struct {
	buf    []byte
	buffer *bytes.Buffer
	last   record.Type
	eo     int64 // end offset of entry
	from   int64
	cb     OnEntryCallback
}

func (s *logStream) Range(ctx context.Context, from int64, cb OnEntryCallback) (int64, error) {
	if len(s.stream) == 0 {
		if from == 0 {
			return 0, nil
		}
		return -1, ErrOutOfRange
	}
	if from < s.firstFile().so || from > s.lastFile().eo {
		return -1, ErrOutOfRange
	}

	sCtx := scanContext{
		buf:    directio.AlignedBlock(int(s.blockSize)),
		buffer: bytes.NewBuffer(nil),
		last:   record.Zero,
		eo:     from,
		from:   from,
		cb:     cb,
	}

	for i, f := range s.stream {
		// log file is compacted, skip.
		if f.eo <= from {
			continue
		}

		err := s.scanFile(ctx, &sCtx, f)
		if err == nil {
			continue
		}

		if errors.Is(err, errEndOfLog) {
			// TODO(james.yin): has empty log file(s).
			if i != len(s.stream)-1 {
				panic("has empty log file")
			}

			// TODO(james.yin): Has incomplete entry, truncate it.
			if sCtx.last.IsNonTerminal() {
				log.Info(context.Background(), "Found incomplete entry, truncate it.",
					map[string]interface{}{
						"last_type": sCtx.last,
					})
			}

			return sCtx.eo, nil
		}

		return -1, err
	}

	panic("WAL: no zero record, the WAL is incomplete.")
}

func (s *logStream) scanFile(ctx context.Context, sCtx *scanContext, lf *logFile) (err error) {
	_, span := s.tracer.Start(ctx, "scanFile")
	defer span.End()

	f, err := io.OpenFile(lf.path, false, false)
	if err != nil {
		return err
	}

	defer func() {
		if err2 := f.Close(); err2 != nil {
			log.Error(context.Background(), "Close file failed.", map[string]interface{}{
				"path":       lf.path,
				log.KeyError: err2,
			})
			err = errutil.Chain(err, err2)
		}
	}()

	for at := s.firstBlockOffset(lf.so, lf.size, sCtx.from); at < lf.size; at += s.blockSize {
		if _, err = f.ReadAt(sCtx.buf, at); err != nil {
			return err
		}

		bso := lf.so + at
		for so := s.firstRecordOffset(lf.so+at, sCtx.from); so <= s.blockSize-record.HeaderSize; {
			r, err2 := record.Unmarshal(sCtx.buf[so:])
			if err2 != nil {
				// TODO(james.yin): handle parse error
				err = err2
				return
			}

			// no new record
			if r.Type == record.Zero {
				err = errEndOfLog
				return
			}

			// TODO(james.yin): check crc

			sz := int64(r.Size())
			reo := bso + so + sz
			if err = onRecord(sCtx, r, reo); err != nil {
				return err
			}
			so += sz
		}
	}

	return nil
}

func onRecord(ctx *scanContext, r record.Record, eo int64) error {
	switch r.Type {
	case record.Full:
		if !ctx.last.IsTerminal() && ctx.last != record.Zero {
			// TODO(james.yin): unexcepted state
			panic("WAL: unexcepted state")
		}
		if err := ctx.cb(r.Data, Range{SO: ctx.eo, EO: eo}); err != nil {
			return err
		}
	case record.First:
		if !ctx.last.IsTerminal() && ctx.last != record.Zero {
			// TODO(james.yin): unexcepted state
			panic("WAL: unexcepted state")
		}
		ctx.buffer.Write(r.Data)
	case record.Middle:
		if !ctx.last.IsNonTerminal() {
			// TODO(james.yin): unexcepted state
			panic("WAL: unexcepted state")
		}
		ctx.buffer.Write(r.Data)
	case record.Last:
		if !ctx.last.IsNonTerminal() {
			// TODO(james.yin): unexcepted state
			panic("WAL: unexcepted state")
		}
		ctx.buffer.Write(r.Data)
		if err := ctx.cb(ctx.buffer.Bytes(), Range{SO: ctx.eo, EO: eo}); err != nil {
			return err
		}
		ctx.buffer.Reset()
	case record.Zero:
		panic("WAL: unexcepted state")
	}

	ctx.last = r.Type
	if ctx.last.IsTerminal() {
		ctx.eo = eo
	}

	return nil
}

func (s *logStream) firstBlockOffset(so, size, from int64) int64 {
	if so < from {
		if so+size <= from {
			panic("WAL: so is out of range.")
		}
		off := from - so
		off -= off % s.blockSize
		return off
	}
	return 0
}

func (s *logStream) firstRecordOffset(so, from int64) int64 {
	if so < from {
		if from-so >= s.blockSize {
			panic("WAL: so is out of range.")
		}
		return from % s.blockSize
	}
	return 0
}

// compact compacts all log files whose end offset is not after off.
func (s *logStream) compact(off int64) error {
	var compacted []*logFile
	defer func() {
		if compacted != nil {
			go doCompact(compacted)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	sz := len(s.stream)
	if sz <= 1 {
		return nil
	}

	for i, f := range s.stream[:sz-1] {
		if f.eo > off {
			if i > 0 {
				compacted = s.stream[:i]
				s.stream = s.stream[i:]
			}
			return nil
		}
	}
	compacted = s.stream[:sz-1]
	s.stream = s.stream[sz-1:]
	return nil
}

func doCompact(files []*logFile) {
	for _, f := range files {
		_ = f.Close()
		_ = os.Remove(f.path)
	}
}
