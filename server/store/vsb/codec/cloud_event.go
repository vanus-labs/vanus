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

package codec

import (
	// standard libraries.
	"encoding/binary"
	"time"

	// this project.
	"github.com/vanus-labs/vanus/server/store/block"
	ceschema "github.com/vanus-labs/vanus/server/store/schema/ce"
)

const (
	byteAligned   int = 8
	alignAddition     = byteAligned - 1
	alignMask         = -byteAligned
	baseAttrSize      = refSize
	timeAttrSize      = refSize + 8
)

type ceEntryEncoder struct{}

// Make sure ceEntryEncoder implements RecordDataEncoder.
var _ RecordDataEncoder = (*ceEntryEncoder)(nil)

type sizeOptAttrCallback struct {
	size int
}

// Make sure sizeOptAttrCallback implements block.OptionalAttributeCallback.
var _ block.OptionalAttributeCallback = (*sizeOptAttrCallback)(nil)

func (cb *sizeOptAttrCallback) OnBytes(_ int, val []byte) {
	cb.size += refSize + alignment(len(val))
}

func (cb *sizeOptAttrCallback) OnString(_ int, val string) {
	cb.size += refSize + alignment(len(val))
}

func (cb *sizeOptAttrCallback) OnUint16(_ int, _ uint16) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnUint64(_ int, _ uint64) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnInt64(_ int, _ int64) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnTime(_ int, _ time.Time) {
	cb.size += timeAttrSize
}

func (cb *sizeOptAttrCallback) OnAttribute(_ int, val interface{}) {
	switch val.(type) {
	case bool, int, int8, int16, int32, uint, uint8, uint32, float32, float64:
		cb.size += baseAttrSize
	default:
		panic("not supported type")
	}
}

type sizeExtAttrCallback struct {
	size int
}

// Make sure sizeExtAttrCallback implements block.ExtensionAttributesCallback.
var _ block.ExtensionAttributeCallback = (*sizeExtAttrCallback)(nil)

func (cb *sizeExtAttrCallback) OnAttribute(attr []byte, val block.Value) {
	cb.size += 16 + alignment(len(attr)) + alignment(val.Size())
}

func (e *ceEntryEncoder) Size(entry block.Entry) int {
	ext, _ := entry.(block.EntryExt)

	sz := 8 // ext count + non-null bitmap

	optCb := &sizeOptAttrCallback{}
	ext.RangeOptionalAttributes(optCb)
	sz += optCb.size

	extCb := &sizeExtAttrCallback{}
	ext.RangeExtensionAttributes(extCb)
	sz += extCb.size

	return sz
}

type optAttrMarshaler struct {
	buf       []byte
	bitmap    uint64
	nextAlloc int
}

// Make sure marshalOptAttrCallback implements block.OptionalAttributeCallback.
var _ block.OptionalAttributeCallback = (*optAttrMarshaler)(nil)

func newOptAttrMarshaler(buf []byte, optCnt, extCnt int) *optAttrMarshaler {
	return &optAttrMarshaler{
		buf:       buf,
		nextAlloc: vlvrOffset(optCnt, extCnt),
	}
}

func (oam *optAttrMarshaler) marshal(e block.EntryExt) (uint64, int) {
	e.RangeOptionalAttributes(oam)
	return oam.bitmap, oam.nextAlloc
}

func (oam *optAttrMarshaler) OnBytes(ordinal int, _ []byte) {
	oam.bitmap |= 1 << ordinal

	if ordinal == ceschema.DataOrdinal {
		return
	}

	// TODO(james.yin):
	panic("not supported type")
}

func (oam *optAttrMarshaler) OnString(ordinal int, val string) {
	oam.bitmap |= 1 << ordinal

	var fo int
	switch ordinal {
	case ceschema.IDOrdinal, ceschema.SourceOrdinal, ceschema.SpecVersionOrdinal, ceschema.TypeOrdinal:
		fo = valueOffset(ordinal)
	case ceschema.DataOrdinal:
		return
	default:
		idx := doValueIndex(oam.bitmap, 1<<ordinal)
		fo = valueOffset(idx)
	}

	field := oam.buf[fo : fo+8]
	offsetAndSize := uint64(oam.nextAlloc)<<offsetOffset | uint64(len(val))
	binary.LittleEndian.PutUint64(field, offsetAndSize)
	copy(oam.buf[oam.nextAlloc:], val)
	oam.nextAlloc += alignment(len(val))
}

func (oam *optAttrMarshaler) OnUint16(ordinal int, _ uint16) {
	oam.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

func (oam *optAttrMarshaler) OnUint64(ordinal int, _ uint64) {
	oam.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

func (oam *optAttrMarshaler) OnInt64(ordinal int, val int64) {
	oam.bitmap |= 1 << ordinal

	var fo int
	switch ordinal {
	case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal:
		fo = valueOffset(ordinal)
	default:
		idx := doValueIndex(oam.bitmap, 1<<ordinal)
		fo = valueOffset(idx)
	}

	field := oam.buf[fo : fo+8]
	binary.LittleEndian.PutUint64(field, uint64(val))
}

func (oam *optAttrMarshaler) OnTime(ordinal int, val time.Time) {
	oam.bitmap |= 1 << ordinal

	idx := doValueIndex(oam.bitmap, 1<<ordinal)
	fo := valueOffset(idx)
	field := oam.buf[fo : fo+8]
	offsetAndNano := uint64(oam.nextAlloc)<<offsetOffset | uint64(val.Nanosecond())&sizeMask
	binary.LittleEndian.PutUint64(field, offsetAndNano)
	binary.LittleEndian.PutUint64(oam.buf[oam.nextAlloc:], uint64(val.Unix()))
	oam.nextAlloc += 8
}

func (oam *optAttrMarshaler) OnAttribute(ordinal int, _ interface{}) {
	oam.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

type extAttrMarshaler struct {
	buf        []byte
	valCache   []block.Value
	i          int
	baseOffset int
	nextAlloc  int
}

// Make sure marshalExtAttrCallback implements block.ExtensionAttributesCallback.
var _ block.ExtensionAttributeCallback = (*extAttrMarshaler)(nil)

func newExtAttrMarshaler(buf []byte, optCnt, extCnt, nextAlloc int) *extAttrMarshaler {
	return &extAttrMarshaler{
		buf:        buf,
		valCache:   make([]block.Value, extCnt),
		baseOffset: valueOffset(optCnt),
		nextAlloc:  nextAlloc,
	}
}

func (eam *extAttrMarshaler) marshal(e block.EntryExt) int {
	// marshal attr key
	e.RangeExtensionAttributes(eam)

	// marshal attr value
	eam.marshalAttrValue()

	return eam.nextAlloc
}

func (eam *extAttrMarshaler) OnAttribute(attr []byte, val block.Value) {
	// cache attr value
	eam.valCache[eam.i] = val

	// marshal attr key
	fo := attrKeyOffset(eam.baseOffset, eam.i)
	field := eam.buf[fo : fo+8]
	binary.LittleEndian.PutUint64(field, makeRef(eam.nextAlloc, len(attr)))
	copy(eam.buf[eam.nextAlloc:], attr)
	eam.nextAlloc += alignment(len(attr))
	eam.i++
}

func (eam *extAttrMarshaler) marshalAttrValue() {
	for i := 0; i < len(eam.valCache); i++ {
		val := eam.valCache[i]
		fo := attrValueOffset(eam.baseOffset, i)
		field := eam.buf[fo : fo+8]
		n := val.Size()
		binary.LittleEndian.PutUint64(field, makeRef(eam.nextAlloc, n))
		if m, ok := val.(block.ValueMarshaler); ok {
			_ = m.MarshalTo(eam.buf[eam.nextAlloc:])
		} else {
			copy(eam.buf[eam.nextAlloc:], val.Value())
		}
		eam.nextAlloc += alignment(n)
	}
}

func (e *ceEntryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	ext, _ := entry.(block.EntryExt)
	optCnt := ext.OptionalAttributeCount()
	extCnt := ext.ExtensionAttributeCount()

	// fill opt attr (exclude data)
	bitmap, nextAlloc := newOptAttrMarshaler(buf, optCnt, extCnt).marshal(ext)

	// fill ext count and non-null attributes bitmap
	binary.LittleEndian.PutUint64(buf[:8], bitmap<<16|uint64(extCnt))

	// fill ext attr
	if extCnt != 0 {
		nextAlloc = newExtAttrMarshaler(buf, optCnt, extCnt, nextAlloc).marshal(ext)
	}

	// fill data
	if bitmap&(1<<ceschema.DataOrdinal) != 0 {
		data := ext.GetBytes(ceschema.DataOrdinal)
		fo := valueOffset(ceschema.DataOrdinal)
		field := buf[fo : fo+8]
		binary.LittleEndian.PutUint64(field, makeRef(nextAlloc, len(data)))
		copy(buf[nextAlloc:], data)
		nextAlloc += alignment(len(data))
	}

	return nextAlloc, 0, nil
}

func alignment(n int) int {
	return (n + alignAddition) & alignMask
}

// vlvrOffset returns the offset of Variable Length Values Region.
func vlvrOffset(valCnt, attrCnt int) int {
	return attrKeyOffset(valueOffset(valCnt), attrCnt)
}
