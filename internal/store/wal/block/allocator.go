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

type Allocator struct {
	size     int
	next     int64
	emptyBuf []byte
	pool     sync.Pool
}

func NewAllocator(size int, so int64) *Allocator {
	a := &Allocator{
		size:     size,
		next:     so,
		emptyBuf: make([]byte, size),
	}
	a.pool = sync.Pool{
		New: func() interface{} {
			return a.rawAlloc()
		},
	}
	return a
}

func (a *Allocator) BlockSize() int {
	return a.size
}

func (a *Allocator) rawAlloc() *Block {
	buf := directio.AlignedBlock(a.size)
	return &Block{
		block: block{
			buf: buf,
		},
	}
}

func (a *Allocator) Next() *Block {
	b, _ := a.pool.Get().(*Block)
	// Reset block.
	copy(b.buf, a.emptyBuf)
	b.wp = 0
	b.fp = 0
	b.cp = 0
	b.SO = a.next
	a.next += int64(a.size)
	return b
}

func (a *Allocator) Free(b *Block) {
	a.pool.Put(b)
}
