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
	"bytes"
	"os"

	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

type WalkFunc func(entry []byte) error

type logStream []logFile

func (ls logStream) lastFile() logFile {
	return ls[len(ls)-1]
}

func (ls logStream) Visit(visitor WalkFunc) error {
	buf := make([]byte, blockSize)
	lastType := record.Zero
	buffer := bytes.NewBuffer(nil)

	for _, lf := range ls {
		file, err := os.Open(lf.path)
		if err != nil {
			return err
		}

		for {
			n, err := file.Read(buf)
			if err != nil {
				return err
			}
			if n != blockSize {
				// TODO(james.yin): incomplete block
			}

			// TODO(james.yin): switch log

			for so := 0; so < blockSize; {
				r, err := record.Unmashal(buf[so:])
				if err != nil {
					// TODO(james.yin): handle parse error
				}

				// TODO(james.yin): check crc

				if r.Type == record.Zero {
					// switch block
					if so > blockSize-record.HeaderSize {
						break
					}

					// TODO(james.yin): no new record
				}

				switch r.Type {
				case record.Full:
					if !lastType.IsTerminal() {
						// TODO(james.yin): unexcepted state
						if lastType == record.Zero {

						}
					}
					if err := visitor(r.Data); err != nil {
						return err
					}
				case record.First:
					if !lastType.IsTerminal() {
						// TODO(james.yin): unexcepted state
						if lastType == record.Zero {

						}
					}
					buffer.Write(r.Data)
				case record.Middle:
					if !lastType.IsNonTerminal() {
						// TODO(james.yin): unexcepted state
					}
					buffer.Write(r.Data)
				case record.Last:
					if !lastType.IsNonTerminal() {
						// TODO(james.yin): unexcepted state
					}
					buffer.Write(r.Data)
					if err := visitor(buffer.Bytes()); err != nil {
						return err
					}
					buffer.Reset()
				}

				lastType = r.Type
				so += r.Size()
			}
		}
	}

	return nil
}
