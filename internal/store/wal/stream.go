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
	"sort"

	// third-party libraries.
	"github.com/ncw/directio"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
	"github.com/linkall-labs/vanus/observability/log"
)

type WalkFunc func(entry []byte, offset int64) error

type logStream struct {
	stream    []*logFile
	dir       string
	blockSize int64
	fileSize  int64
}

func (s *logStream) lastFile() *logFile {
	if len(s.stream) == 0 {
		return nil
	}
	return s.stream[len(s.stream)-1]
}

func (s *logStream) selectFile(offset int64, autoCreate bool) *logFile {
	if len(s.stream) == 0 {
		if offset == 0 {
			if !autoCreate {
				return nil
			}
			return s.createNextFile(nil)
		}
		panic("log stream not begin from 0")
	}

	last := s.lastFile()
	if offset >= last.so {
		eo := last.so + last.size
		if offset < eo {
			return last
		}
		if offset == eo {
			if !autoCreate {
				return nil
			}
			return s.createNextFile(last)
		}
		panic("log stream overflow")
	}
	i := sort.Search(len(s.stream)-1, func(i int) bool {
		f := s.stream[i]
		eo := f.so + f.size
		return eo > offset
	})
	if i < len(s.stream)-1 {
		return s.stream[i]
	}

	panic("log stream underflow")
}

func (s *logStream) createNextFile(last *logFile) *logFile {
	off := func() int64 {
		if last != nil {
			return last.so + last.size
		}
		return 0
	}()
	next, err := createLogFile(s.dir, off, s.fileSize, true)
	if err != nil {
		panic(err)
	}
	s.stream = append(s.stream, next)
	return next
}

type scanContext struct {
	buf       []byte
	buffer    *bytes.Buffer
	lastType  record.Type
	nextStart int64
	compacted int64
}

var errEndOfLog = errors.New("WAL: end of log")

func (s *logStream) Visit(visitor WalkFunc, compacted int64) (int64, error) {
	if len(s.stream) == 0 {
		return compacted, nil
	}

	ctx := scanContext{
		buf:       directio.AlignedBlock(int(s.blockSize)),
		buffer:    bytes.NewBuffer(nil),
		lastType:  record.Zero,
		nextStart: compacted,
		compacted: compacted,
	}

	for i, f := range s.stream {
		// log file is compacted, skip.
		if f.so+f.size <= compacted {
			continue
		}

		err := s.scanFile(&ctx, f, visitor)

		if err == nil {
			continue
		}

		if errors.Is(err, errEndOfLog) {
			// TODO(james.yin): has empty log file(s).
			if i != len(s.stream)-1 {
				panic("has empty log file")
			}

			// TODO(james.yin): Has incomplete entry, truncate it.
			if ctx.lastType.IsNonTerminal() {
				log.Info(context.Background(), "Found incomplete entry, truncate it.",
					map[string]interface{}{
						"lastType": ctx.lastType,
					})
			}

			return ctx.nextStart, nil
		}

		return -1, err
	}

	panic("WAL: no zero record, the WAL is incomplete.")
}

func (s *logStream) scanFile(ctx *scanContext, lf *logFile, visitor WalkFunc) error {
	f, err := openFile(lf.path)
	if err != nil {
		return err
	}
	defer func() {
		if err2 := f.Close(); err2 != nil {
			log.Error(context.Background(), "Close file failed.", map[string]interface{}{
				"path":       lf.path,
				log.KeyError: err2,
			})
		}
	}()

	for at := s.firstBlockOffset(lf.so, lf.size, ctx.compacted); at < lf.size; at += s.blockSize {
		if _, err = f.ReadAt(ctx.buf, at); err != nil {
			return err
		}

		for so := s.firstRecordOffset(lf.so+at, ctx.compacted); so <= s.blockSize-record.HeaderSize; {
			r, err2 := record.Unmarshal(ctx.buf[so:])
			if err2 != nil {
				// TODO(james.yin): handle parse error
				return err2
			}

			// no new record
			if r.Type == record.Zero {
				return errEndOfLog
			}

			// TODO(james.yin): check crc

			sz := int64(r.Size())
			switch r.Type {
			case record.Full:
				if !ctx.lastType.IsTerminal() && ctx.lastType != record.Zero {
					// TODO(james.yin): unexcepted state
					panic("WAL: unexcepted state")
				}
				offset := lf.so + at + so + sz
				if err3 := visitor(r.Data, offset); err3 != nil {
					return err3
				}
			case record.First:
				if !ctx.lastType.IsTerminal() && ctx.lastType != record.Zero {
					// TODO(james.yin): unexcepted state
					panic("WAL: unexcepted state")
				}
				ctx.buffer.Write(r.Data)
			case record.Middle:
				if !ctx.lastType.IsNonTerminal() {
					// TODO(james.yin): unexcepted state
					panic("WAL: unexcepted state")
				}
				ctx.buffer.Write(r.Data)
			case record.Last:
				if !ctx.lastType.IsNonTerminal() {
					// TODO(james.yin): unexcepted state
					panic("WAL: unexcepted state")
				}
				ctx.buffer.Write(r.Data)
				offset := lf.so + at + so + sz
				if err3 := visitor(ctx.buffer.Bytes(), offset); err3 != nil {
					return err3
				}
				ctx.buffer.Reset()
			case record.Zero:
				panic("WAL: unexcepted state")
			}

			ctx.lastType = r.Type
			so += sz
			if ctx.lastType.IsTerminal() {
				ctx.nextStart = lf.so + at + so
			}
		}
	}

	return nil
}

func (s *logStream) firstBlockOffset(so, size, compacted int64) int64 {
	if so < compacted {
		if so+size <= compacted {
			panic("WAL: so is out of range.")
		}
		off := compacted - so
		off -= off % s.blockSize
		return off
	}
	return 0
}

func (s *logStream) firstRecordOffset(so, compacted int64) int64 {
	if so < compacted {
		if compacted-so >= s.blockSize {
			panic("WAL: so is out of range.")
		}
		return compacted % s.blockSize
	}
	return 0
}
