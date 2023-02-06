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
	"sync"

	// third-party libraries.
	"github.com/ncw/directio"
)

type BufferPool struct {
	size     int
	emptyBuf []byte
	pool     sync.Pool
}

func NewBufferPool(size int) *BufferPool {
	a := &BufferPool{
		size:     size,
		emptyBuf: make([]byte, size),
	}
	a.pool = sync.Pool{
		New: func() interface{} {
			return a.rawAlloc()
		},
	}
	return a
}

func (a *BufferPool) BufferSize() int {
	return a.size
}

func (a *BufferPool) rawAlloc() *Buffer {
	buf := directio.AlignedBlock(a.size)
	return &Buffer{
		buf: buf,
	}
}

func (a *BufferPool) Get(base int64) *Buffer {
	b, _ := a.pool.Get().(*Buffer)
	// Reset block.
	b.base = base
	copy(b.buf, a.emptyBuf)
	b.wp = 0
	b.fp = 0
	b.cp = 0
	return b
}

func (a *BufferPool) Put(b *Buffer) {
	a.pool.Put(b)
}
