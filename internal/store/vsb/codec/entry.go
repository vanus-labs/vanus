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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, wther express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	// standard libraries.
	"bytes"
	"encoding/binary"
	"math/bits"
	"sort"
	"time"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

const (
	offsetOffset        = 4 * 8
	sizeMask     uint64 = 0x00000000FFFFFFFF

	extAttrCountSize = 2
	bitmapSize       = 6
	entryHeaderSize  = extAttrCountSize + bitmapSize

	bitmapOffset = extAttrCountSize * 8

	refSize = 8

	optAttrSize      = refSize
	extAttrKeySize   = refSize
	extAttrValueSize = refSize
	extAttrPairSize  = extAttrKeySize + extAttrValueSize

	extAttrKeyOffset   = 0
	extAttrValueOffset = extAttrKeyOffset + extAttrKeySize
)

type entry struct {
	t    uint16
	data []byte
}

// Make sure entry implements block.Entry.
var _ block.Entry = (*entry)(nil)

func (e *entry) Get(ordinal int) interface{} {
	return nil
}

func (e *entry) GetBytes(ordinal int) []byte {
	idx := e.valueIndex(ordinal)
	if idx < 0 {
		return nil
	}
	vo := valueOffset(idx)
	return e.deref(vo)
}

func (e *entry) GetString(ordinal int) string {
	idx := e.valueIndex(ordinal)
	if idx < 0 {
		return ""
	}
	vo := valueOffset(idx)
	return string(e.deref(vo))
}

func (e *entry) GetUint16(ordinal int) uint16 {
	if ordinal == ceschema.EntryTypeOrdinal {
		return e.t
	}

	idx := e.valueIndex(ordinal)
	if idx < 0 {
		return 0
	}
	vo := valueOffset(idx)
	return binary.LittleEndian.Uint16(e.data[vo:])
}

func (e *entry) GetUint64(ordinal int) uint64 {
	idx := e.valueIndex(ordinal)
	if idx < 0 {
		return 0
	}
	vo := valueOffset(idx)
	return binary.LittleEndian.Uint64(e.data[vo:])
}

func (e *entry) GetInt64(ordinal int) int64 {
	return int64(e.GetUint64(ordinal))
}

func (e *entry) GetTime(ordinal int) time.Time {
	idx := e.valueIndex(ordinal)
	if idx < 0 {
		return time.Time{}
	}
	vo := valueOffset(idx)
	off, nano := offsetAndLength(e.data[vo:])
	sec := binary.LittleEndian.Uint64(e.data[off:])
	return time.Unix(int64(sec), int64(nano))
}

func (e *entry) ExtensionAttributeCount() int {
	return e.extCount()
}

func (e *entry) GetExtensionAttribute(attr []byte) []byte {
	sz := e.extCount()
	if sz == 0 {
		return nil
	}
	base := e.extVecBase()
	idx := sort.Search(sz, func(i int) bool {
		return bytes.Compare(attr, e.deref(attrKeyOffset(base, i))) <= 0
	})
	if idx < sz && bytes.Equal(attr, e.deref(attrKeyOffset(base, idx))) {
		return e.deref(attrValueOffset(base, idx))
	}
	return nil
}

func (e *entry) RangeExtensionAttributes(f func(attr, val []byte)) {
	sz := e.extCount()
	if sz == 0 {
		return
	}

	base := e.extVecBase()
	for i := 0; i < sz; i++ {
		attr := e.deref(attrKeyOffset(base, i))
		val := e.deref(attrValueOffset(base, i))
		f(attr, val)
	}
}

func (e *entry) bitmap() uint64 {
	return binary.LittleEndian.Uint64(e.data) >> bitmapOffset
}

func (e *entry) valueCount() int {
	return bits.OnesCount64(e.bitmap())
}

func (e *entry) valueIndex(ordinal int) int {
	return valueIndex(e.bitmap(), uint64(1)<<ordinal)
}

func (e *entry) extCount() int {
	return int(binary.LittleEndian.Uint16(e.data))
}

func (e *entry) extVecBase() int {
	return valueOffset(e.valueCount())
}

func (e *entry) deref(base int) []byte {
	off, sz := offsetAndLength(e.data[base:])
	return e.data[off : off+sz]
}

func offsetAndLength(data []byte) (uint32, uint32) {
	offsetAndSize := binary.LittleEndian.Uint64(data)
	off := offsetAndSize >> offsetOffset
	sz := offsetAndSize & sizeMask
	return uint32(off), uint32(sz)
}

func valueIndex(bitmap uint64, mask uint64) int {
	if bitmap&mask == 0 {
		return -1
	}
	return doValueIndex(bitmap, mask)
}

func doValueIndex(bitmap uint64, mask uint64) int {
	return bits.OnesCount64(bitmap &^ (^mask + 1))
}

func valueOffset(idx int) int {
	return entryHeaderSize + idx*optAttrSize
}

func attrKeyOffset(base int, idx int) int {
	return base + idx*extAttrPairSize + extAttrKeyOffset
}

func attrValueOffset(base int, idx int) int {
	return base + idx*extAttrPairSize + extAttrValueOffset
}

type entryEncoder struct {
	ceEnc    ceEntryEncoder
	endEnc   endEntryEncoder
	indexEnc indexEntryEncoder
}

// Make sure entryEncoder implements RecordDataEncoder.
var _ RecordDataEncoder = (*entryEncoder)(nil)

func (e *entryEncoder) Size(entry block.Entry) int {
	switch ceschema.EntryType(entry) {
	case ceschema.CloudEvent:
		return e.ceEnc.Size(entry)
	case ceschema.End:
		return e.endEnc.Size(entry)
	case ceschema.Index:
		return e.indexEnc.Size(entry)
	}
	return -1
}

func (e *entryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	switch ceschema.EntryType(entry) {
	case ceschema.CloudEvent:
		return e.ceEnc.MarshalTo(entry, buf)
	case ceschema.End:
		return e.endEnc.MarshalTo(entry, buf)
	case ceschema.Index:
		return e.indexEnc.MarshalTo(entry, buf)
	}
	return 0, 0, ErrUnknownRecord
}

type entryDecoder struct {
	indexDec indexEntryDecoder
}

// Make sure entryDecoder implements RecordDataDecoder.
var _ RecordDataDecoder = (*entryDecoder)(nil)

func (d *entryDecoder) Unmarshal(t uint16, offset int, data []byte) (block.Entry, error) {
	switch t {
	case ceschema.CloudEvent, ceschema.End:
		return &entry{t: t, data: data[offset:]}, nil
	case ceschema.Index:
		return d.indexDec.Unmarshal(t, offset, data)
	}
	return nil, ErrUnknownRecord
}
