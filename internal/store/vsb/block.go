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

package vsb

import (
	// standard libraries.
	"context"
	"os"
	"sync"
	"sync/atomic"

	// first-party.
	"github.com/linkall-labs/vanus/observability/tracing"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/vsb/codec"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

const FormatMagic = uint32(0x00627376) // ASCII of "vsb" in little endian

type meta struct {
	writeOffset int64
	// entryLength is the length of persisted entries.
	entryLength int64
	// entryNum is the number of persisted entries.
	entryNum int64
	// archived is the flag indicating Block is archived.
	archived bool
}

// vsBlock is Vanus block file.
type vsBlock struct {
	id       vanus.ID
	path     string
	capacity int64

	dataOffset int64
	indexSize  uint16

	indexOffset int64
	indexLength int

	fm      meta // flushed meta
	actx    appendContext
	indexes []index.Index
	mu      sync.RWMutex

	enc codec.EntryEncoder
	dec codec.EntryDecoder
	lis block.ArchivedListener

	f      *os.File
	wg     sync.WaitGroup
	tracer *tracing.Tracer
}

// Make sure vsBlock implements block.File.
var _ block.Raw = (*vsBlock)(nil)

func (b *vsBlock) ID() vanus.ID {
	return b.id
}

func (b *vsBlock) Close(ctx context.Context) error {
	b.wg.Wait()

	m, indexes := b.makeSnapshot()

	if b.indexOffset != m.writeOffset {
		n, err := b.appendIndexEntry(indexes, m.writeOffset)
		if err != nil {
			return err
		}
		b.indexOffset = m.writeOffset
		b.indexLength = n
	}

	// Flush metadata.
	if b.fm.archived != m.archived || b.fm.entryLength != m.entryLength {
		if err := b.persistHeader(ctx, m); err != nil {
			return err
		}
	}

	return b.f.Close()
}

func (b *vsBlock) Delete(context.Context) error {
	// FIXME(james.yin): make sure block is closed.
	return os.Remove(b.path)
}

func (b *vsBlock) status() block.Statistics {
	return b.stat(b.makeSnapshot())
}

func (b *vsBlock) stat(m meta, indexes []index.Index) block.Statistics {
	s := block.Statistics{
		ID:              b.id,
		Capacity:        uint64(b.capacity),
		Archived:        m.archived,
		EntryNum:        uint32(m.entryNum),
		EntrySize:       uint64(m.entryLength),
		FirstEntryStime: -1,
		LastEntryStime:  -1,
	}
	if sz := len(indexes); sz != 0 {
		s.FirstEntryStime = indexes[0].Stime()
		s.LastEntryStime = indexes[sz-1].Stime()
	}
	return s
}

func (b *vsBlock) full() bool {
	return atomic.LoadUint32(&b.actx.archived) != 0
}
