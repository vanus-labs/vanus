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
	"io"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

type block struct {
	buf []byte
	// wp is write pointer
	wp int
	// fp is flush pointer
	fp int
}

func (b *block) Capacity() int {
	return len(b.buf)
}

func (b *block) Size() int {
	return b.wp
}

func (b *block) Remaining() int {
	return b.remaining(b.Size())
}

func (b *block) remaining(offset int) int {
	return b.Capacity() - offset
}

func (b *block) Full() bool {
	return b.Remaining() < record.RecordHeaderSize
}

func (b *block) full(off int) bool {
	return b.remaining(off) < record.RecordHeaderSize
}

func (b *block) Append(r record.Record) (int, error) {
	n, err := r.MarshalTo(b.buf[b.wp:])
	if err != nil {
		return 0, err
	}
	b.wp += n
	return b.wp, nil
}

func (b *block) Flush(writer io.Writer, offset int) (int, error) {
	// Already flushed, skip.
	if b.fp >= offset {
		return b.fp, nil
	}

	n, err := writer.Write(b.buf[b.fp:offset])
	if err != nil {
		return n, err
	}
	b.fp += n
	return b.fp, nil
}
