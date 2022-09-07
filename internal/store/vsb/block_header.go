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
	"encoding/binary"
	"hash/crc32"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block/raw"
)

const (
	headerBlockSize = 4 * 1024
	headerSize      = 4 + 4 + 4 + 4 + 4 + 1 + 1 + 2 + 8 + 8 + 4 + 2

	magicOffset       = 0
	crcOffset         = 4
	flagsOffset       = 8
	breakFlagsOffset  = 12
	dataOffsetOffset  = 16
	stateOffset       = 20
	indexSizeOffset   = 22
	capacityOffset    = 24
	entryLengthOffset = 32
	entryNumOffset    = 40
	indexOffsetOffset = 44
)

var (
	crc32q      = crc32.MakeTable(crc32.Castagnoli)
	emptyHeader = make([]byte, headerBlockSize)
)

func (b *vsBlock) persistHeader(ctx context.Context, m meta) error {
	var buf [headerSize]byte
	binary.LittleEndian.PutUint32(buf[magicOffset:], FormatMagic)               // magic
	binary.LittleEndian.PutUint32(buf[flagsOffset:], 0)                         // flags
	binary.LittleEndian.PutUint32(buf[breakFlagsOffset:], 0)                    // break flags
	binary.LittleEndian.PutUint32(buf[dataOffsetOffset:], uint32(b.dataOffset)) // data offset
	if m.archived {                                                             // state
		buf[stateOffset] = 1
	}
	binary.LittleEndian.PutUint16(buf[indexSizeOffset:], b.indexSize)             // index size
	binary.LittleEndian.PutUint64(buf[capacityOffset:], uint64(b.capacity))       // capacity
	binary.LittleEndian.PutUint64(buf[entryLengthOffset:], uint64(m.entryLength)) // entry length
	binary.LittleEndian.PutUint32(buf[entryNumOffset:], uint32(m.entryNum))       // entry number
	if eo := b.dataOffset + m.entryLength; b.indexOffset > eo {                   // index offset
		off := b.indexOffset - eo
		binary.LittleEndian.PutUint16(buf[indexOffsetOffset:], uint16(off))
	}
	crc := crc32.Checksum(buf[flagsOffset:], crc32q)
	crc = crc32.Update(crc, crc32q, emptyHeader[headerSize:])
	binary.LittleEndian.PutUint32(buf[crcOffset:], crc) // crc

	if _, err := b.f.WriteAt(buf[:], 0); err != nil {
		return err
	}

	b.mu.Lock()
	b.fm = m
	b.mu.Unlock()

	return nil
}

func (b *vsBlock) loadHeader(ctx context.Context) error {
	var buf [headerSize]byte
	if _, err := b.f.ReadAt(buf[:], 0); err != nil {
		return err
	}

	magic := binary.LittleEndian.Uint32(buf[magicOffset:])
	if magic != FormatMagic {
		return raw.ErrInvalidFormat
	}

	breakFlags := binary.LittleEndian.Uint32(buf[breakFlagsOffset:])
	if breakFlags != 0 {
		return errIncomplete
	}

	b.dataOffset = int64(binary.LittleEndian.Uint32(buf[dataOffsetOffset:]))      // data offset
	b.fm.archived = buf[stateOffset] != 0                                         // state
	b.indexSize = binary.LittleEndian.Uint16(buf[indexSizeOffset:])               // index size
	b.capacity = int64(binary.LittleEndian.Uint64(buf[capacityOffset:]))          // capacity
	b.fm.entryLength = int64(binary.LittleEndian.Uint64(buf[entryLengthOffset:])) // entry length
	b.fm.entryNum = int64(binary.LittleEndian.Uint32(buf[entryNumOffset:]))       // entry number

	origin := binary.LittleEndian.Uint32(buf[crcOffset:])
	crc := crc32.Checksum(buf[flagsOffset:], crc32q)
	crc = crc32.Update(crc, crc32q, emptyHeader[headerSize:])
	if origin != crc {
		return errCorrupted
	}

	return nil
}
