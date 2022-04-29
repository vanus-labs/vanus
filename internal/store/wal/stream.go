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
	"sort"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
	"github.com/linkall-labs/vanus/observability/log"
)

type WalkFunc func(entry []byte, offset int64) error

type logStream struct {
	stream []*logFile
	dir    string
}

func (s *logStream) lastFile() *logFile {
	if len(s.stream) == 0 {
		return nil
	}
	return s.stream[len(s.stream)-1]
}

func (s *logStream) selectFile(offset int64) *logFile {
	if len(s.stream) == 0 {
		if offset == 0 {
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
	next, err := createLogFile(s.dir, off, fileSize, true)
	if err != nil {
		panic(err)
	}
	s.stream = append(s.stream, next)
	return next
}

func (s *logStream) Visit(visitor WalkFunc, compacted int64) (int64, error) {
	if len(s.stream) == 0 {
		return compacted, nil
	}

	buf := make([]byte, blockSize)
	buffer := bytes.NewBuffer(nil)
	lastType := record.Zero
	nextStart := compacted

	for i, f := range s.stream {
		// log file is compacted
		if f.so+f.size <= compacted {
			continue
		}

		if err := f.Open(); err != nil {
			return -1, err
		}

		for at := firstBlockOffset(f.so, compacted); at < f.size; {
			if _, err := f.f.ReadAt(buf, at); err != nil {
				return -1, err
			}

			for so := 0; so <= blockSize-record.HeaderSize; {
				r, err2 := record.Unmashal(buf[so:])
				if err2 != nil {
					// TODO(james.yin): handle parse error
					return -1, err2
				}

				// no new record
				if r.Type == record.Zero {
					// TODO(james.yin): has empty log file(s).
					if i != len(s.stream)-1 {
					}

					// TODO(james.yin): Has incomplete entry, truncate it.
					if lastType.IsNonTerminal() {
						log.Info(context.TODO(), "Found incomplete entry, truncate it.",
							map[string]interface{}{
								"lastType": lastType,
							})
					}

					return nextStart, nil
				}

				// TODO(james.yin): check crc

				switch r.Type {
				case record.Full:
					if !lastType.IsTerminal() && lastType != record.Zero {
						// TODO(james.yin): unexcepted state
						panic("WAL: unexcepted state")
					}
					offset := f.so + at + int64(so+r.Size())
					if err3 := visitor(r.Data, offset); err3 != nil {
						return -1, err3
					}
				case record.First:
					if !lastType.IsTerminal() && lastType != record.Zero {
						// TODO(james.yin): unexcepted state
						panic("WAL: unexcepted state")
					}
					buffer.Write(r.Data)
				case record.Middle:
					if !lastType.IsNonTerminal() {
						// TODO(james.yin): unexcepted state
						panic("WAL: unexcepted state")
					}
					buffer.Write(r.Data)
				case record.Last:
					if !lastType.IsNonTerminal() {
						// TODO(james.yin): unexcepted state
						panic("WAL: unexcepted state")
					}
					buffer.Write(r.Data)
					offset := f.so + at + int64(so+r.Size())
					if err3 := visitor(buffer.Bytes(), offset); err3 != nil {
						return -1, err3
					}
					buffer.Reset()
				case record.Zero:
					panic("WAL: unexcepted state")
				}

				lastType = r.Type
				so += r.Size()
				if lastType.IsTerminal() {
					nextStart = f.so + at + int64(so)
				}
			}

			at += blockSize
		}

		// TODO(james.yin): close log file
	}

	panic("WAL: no zero record, the WAL is incomplete.")
}

func firstBlockOffset(so, compacted int64) int64 {
	if so < compacted {
		off := compacted - so
		off -= off % blockSize
		return off
	}
	return 0
}
