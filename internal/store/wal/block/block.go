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

package block

import (
	// standard libraries.
	"os"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

type FlushCallback func(off int64, err error)

type block struct {
	buf []byte
	// wp is write pointer
	wp int
	// fp is flush pointer
	fp int
	// cp is commit pointer
	cp int
}

func (b *block) Capacity() int {
	return len(b.buf)
}

func (b *block) Size() int {
	return b.wp
}

func (b *block) Committed() int {
	return b.cp
}

func (b *block) Remaining() int {
	return b.remaining(b.Size())
}

func (b *block) remaining(offset int) int {
	return b.Capacity() - offset
}

func (b *block) Full() bool {
	return b.Remaining() < record.HeaderSize
}

func (b *block) FullWithOff(off int) bool {
	return b.remaining(off) < record.HeaderSize
}

func (b *block) Append(r record.Record) (int, error) {
	n, err := r.MarshalTo(b.buf[b.wp:])
	if err != nil {
		return 0, err
	}
	b.wp += n
	return b.wp, nil
}

func (b *block) Flush(writer io.WriterAt, offset int, base int64, cb FlushCallback) {
	// Already flushed, skip.
	if b.fp >= offset {
		cb(int64(b.fp), nil)
		return
	}
	b.fp = offset

	writer.WriteAt(b.buf, base, func(_ int, err error) {
		if err != nil {
			cb(0, err)
		} else {
			if offset > b.cp {
				b.cp = offset
			}
			cb(int64(b.cp), nil)
		}
	})
}

func (b *block) RecoverFromFile(f *os.File, at int64, committed int) error {
	if _, err := f.ReadAt(b.buf, at); err != nil {
		return err
	}
	b.wp = committed
	b.fp = committed
	b.cp = committed
	return nil
}

type Block struct {
	block
	// SO is start offset
	SO int64
}

func (b *Block) WriteOffset() int64 {
	return b.SO + int64(b.Size())
}
