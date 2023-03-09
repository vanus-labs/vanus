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

	// third-party project.
	"github.com/ncw/directio"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/io/zone/segmentedfile"
	"github.com/vanus-labs/vanus/internal/store/wal/record"
)

type OnEntryCallback = func(entry []byte, r Range) error

var (
	ErrOutOfRange = errors.New("WAL: out of range")
	errEndOfLog   = errors.New("WAL: end of log")
)

func scanLogEntries(sf *segmentedfile.SegmentedFile, blockSize int, from int64, cb OnEntryCallback) (int64, error) {
	s := sf.SelectSegment(from, false)
	if s == nil {
		if from == 0 {
			return 0, nil
		}
		return -1, ErrOutOfRange
	}

	if cb == nil {
		cb = noopOnEntry
	}

	sc := scanner{
		blockSize: int64(blockSize),
		buf:       directio.AlignedBlock(blockSize),
		buffer:    bytes.NewBuffer(nil),
		last:      record.Zero,
		eo:        from,
		from:      from,
		cb:        cb,
	}

	for {
		err := sc.scanSegmentFile(s)
		if err == nil {
			s = sf.SelectSegment(s.EO(), false)
			if s == nil {
				break
			}
			continue
		}

		if errors.Is(err, errEndOfLog) {
			// TODO(james.yin): has empty log file(s).
			// if i != len(s.stream)-1 {
			// 	panic("has empty log file")
			// }

			// TODO(james.yin): Has incomplete entry, truncate it.
			if sc.last.IsNonTerminal() {
				log.Info(context.Background(), "Found incomplete entry, truncate it.",
					map[string]interface{}{
						"last_type": sc.last,
					})
			}

			return sc.eo, nil
		}

		return -1, err
	}

	return 0, nil
}

type scanner struct {
	blockSize int64
	buf       []byte
	buffer    *bytes.Buffer
	last      record.Type
	eo        int64 // end offset of entry
	from      int64
	cb        OnEntryCallback
}

func (sc *scanner) scanSegmentFile(s *segmentedfile.Segment) (err error) {
	f := s.File()
	for at := sc.firstBlockOffset(s); at < s.Size(); at += sc.blockSize {
		if _, err = f.ReadAt(sc.buf, at); err != nil {
			return err
		}

		bso := s.SO() + at
		for so := sc.firstRecordOffset(bso); so <= sc.blockSize-record.HeaderSize; {
			r, err2 := record.Unmarshal(sc.buf[so:])
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
			if err = onRecord(sc, r, reo); err != nil {
				return err
			}
			so += sz
		}
	}

	return nil
}

func (sc *scanner) firstBlockOffset(s *segmentedfile.Segment) int64 {
	if s.SO() < sc.from {
		if s.EO() <= sc.from {
			panic("WAL: so is out of range.")
		}
		off := sc.from - s.SO()
		off -= off % sc.blockSize
		return off
	}
	return 0
}

func (sc *scanner) firstRecordOffset(so int64) int64 {
	if so < sc.from {
		if sc.from-so >= sc.blockSize {
			panic("WAL: so is out of range.")
		}
		return sc.from % sc.blockSize
	}
	return 0
}

func onRecord(ctx *scanner, r record.Record, eo int64) error {
	switch r.Type {
	case record.Full:
		if !ctx.last.IsTerminal() && ctx.last != record.Zero {
			// TODO(james.yin): unexpected state
			panic("WAL: unexpected state")
		}
		if err := ctx.cb(r.Data, Range{SO: ctx.eo, EO: eo}); err != nil {
			return err
		}
	case record.First:
		if !ctx.last.IsTerminal() && ctx.last != record.Zero {
			// TODO(james.yin): unexpected state
			panic("WAL: unexpected state")
		}
		ctx.buffer.Write(r.Data)
	case record.Middle:
		if !ctx.last.IsNonTerminal() {
			// TODO(james.yin): unexpected state
			panic("WAL: unexpected state")
		}
		ctx.buffer.Write(r.Data)
	case record.Last:
		if !ctx.last.IsNonTerminal() {
			// TODO(james.yin): unexpected state
			panic("WAL: unexpected state")
		}
		ctx.buffer.Write(r.Data)
		if err := ctx.cb(ctx.buffer.Bytes(), Range{SO: ctx.eo, EO: eo}); err != nil {
			return err
		}
		ctx.buffer.Reset()
	case record.Zero:
		panic("WAL: unexpected state")
	}

	ctx.last = r.Type
	if ctx.last.IsTerminal() {
		ctx.eo = eo
	}

	return nil
}

func noopOnEntry(entry []byte, r Range) error {
	return nil
}
