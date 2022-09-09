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
	"errors"
	"io"
	"math"
	"os"

	// first-party libraries.
	errutil "github.com/linkall-labs/vanus/pkg/util/errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
	"github.com/linkall-labs/vanus/internal/store/vsb/codec"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

var (
	errCorrupted  = errors.New("corrupted vsb")
	errIncomplete = errors.New("incomplete vsb")
)

func (b *vsBlock) Open(ctx context.Context) error {
	if b.f != nil {
		return nil
	}

	// TODO(james.yin): use direct IO
	f, err := os.OpenFile(b.path, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		return err
	}
	b.f = f

	if err = b.init(ctx); err != nil {
		if err2 := f.Close(); err2 != nil {
			return errutil.Chain(err, err2)
		}
		b.f = nil
		return err
	}

	return nil
}

func (b *vsBlock) init(ctx context.Context) error {
	if err := b.loadHeader(ctx); err != nil {
		return err
	}

	b.enc = codec.NewEncoder()
	if dec, err := codec.NewDecoder(false, int(b.indexSize)); err == nil {
		b.dec = dec
	} else {
		return err
	}

	if err := b.repairMeta(); err != nil {
		return err
	}

	if err := b.validate(ctx); err != nil {
		return err
	}

	return nil
}

func (b *vsBlock) repairMeta() error {
	off := b.dataOffset + b.fm.entryLength
	seq := b.fm.entryNum
	full := b.fm.archived

	var entry block.Entry
	var err error
	var n, en int

	// Scan entries.
	indexes := make([]index.Index, 0)
	// Note: use math.MaxInt64-off to avoid overflow.
	r := io.NewSectionReader(b.f, off, math.MaxInt64-off)
	if full {
		n, entry, err = b.dec.UnmarshalReader(r)
		if err != nil || ceschema.EntryType(entry) != ceschema.End {
			return errCorrupted
		}
		goto FOUND_END
	}
	for {
		n, entry, err = b.dec.UnmarshalReader(r)
		if err != nil {
			if err = b.rebuildIndexes(int(seq), indexes); err != nil {
				return err
			}
			goto SET_META
		}

		switch ceschema.EntryType(entry) {
		case ceschema.End:
			goto FOUND_END
		case ceschema.Index:
			goto FOUND_INDEX
		}

		idx := index.NewIndex(off, int32(n), index.WithEntry(entry))
		indexes = append(indexes, idx)

		off += int64(n)
		seq++
	}

FOUND_END:
	if ceschema.SequenceNumber(entry) != seq {
		return errCorrupted
	}

	en = n
	off += int64(n)
	seq++
	full = true

	n, entry, err = b.dec.UnmarshalReader(r)
	if err != nil {
		if err = b.rebuildIndexes(int(seq)-1, indexes); err != nil {
			return err
		}
		goto SET_META
	}
	if ceschema.EntryType(entry) != ceschema.Index {
		return errCorrupted
	}

FOUND_INDEX:
	b.indexes, _ = entry.Get(ceschema.IndexesOrdinal).([]index.Index)
	if sz := len(b.indexes); sz > 0 && b.indexes[sz-1].EndOffset() != off-int64(en) {
		return errCorrupted
	}
	b.indexOffset = off
	b.indexLength = n

SET_META:
	b.fm.writeOffset = off

	b.actx.seq = seq
	b.actx.offset = off
	if full {
		b.actx.archived = 1
	}

	return nil
}

func (b *vsBlock) rebuildIndexes(num int, tail []index.Index) error {
	indexes := make([]index.Index, 0, num)

	// Scan entries.
	off := b.dataOffset
	r := io.NewSectionReader(b.f, off, b.fm.entryLength)
	for {
		n, entry, err := b.dec.UnmarshalReader(r)
		if err != nil {
			if errors.Is(err, codec.ErrIncompletePacket) {
				break
			}
			return errutil.Chain(errCorrupted, err)
		}

		if ceschema.EntryType(entry) != ceschema.CloudEvent {
			return errCorrupted
		}

		idx := index.NewIndex(off, int32(n), index.WithEntry(entry))
		indexes = append(indexes, idx)
	}

	if len(indexes)+len(tail) != num {
		return errCorrupted
	}

	indexes = append(indexes, tail...)
	b.indexes = indexes
	return nil
}

func (b *vsBlock) validate(ctx context.Context) error {
	if len(b.indexes) < int(b.fm.entryNum) {
		return errCorrupted
	}
	return nil
}
