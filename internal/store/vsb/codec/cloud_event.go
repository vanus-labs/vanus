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
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
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

func (cb *sizeOptAttrCallback) OnBytes(ordinal int, val []byte) {
	cb.size += refSize + alignment(len(val))
}

func (cb *sizeOptAttrCallback) OnString(ordinal int, val string) {
	cb.size += refSize + alignment(len(val))
}

func (cb *sizeOptAttrCallback) OnUint16(ordinal int, val uint16) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnUint64(ordinal int, val uint64) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnInt64(ordinal int, val int64) {
	cb.size += baseAttrSize
}

func (cb *sizeOptAttrCallback) OnTime(ordinal int, val time.Time) {
	cb.size += timeAttrSize
}

func (cb *sizeOptAttrCallback) OnAttribute(ordinal int, val interface{}) {
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

func (cb *sizeExtAttrCallback) OnAttribute(attr, val []byte) {
	cb.size += 16 + alignment(len(attr)) + alignment(len(val))
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

type marshalOptAttrCallback struct {
	buf       []byte
	bitmap    uint64
	nextAlloc int
}

// Make sure marshalOptAttrCallback implements block.OptionalAttributeCallback.
var _ block.OptionalAttributeCallback = (*marshalOptAttrCallback)(nil)

func (cb *marshalOptAttrCallback) OnBytes(ordinal int, val []byte) {
	cb.bitmap |= 1 << ordinal

	if ordinal == ceschema.DataOrdinal {
		return
	}

	// TODO(james.yin):
	panic("not supported type")
}

func (cb *marshalOptAttrCallback) OnString(ordinal int, val string) {
	cb.bitmap |= 1 << ordinal

	var fo int
	switch ordinal {
	case ceschema.IDOrdinal, ceschema.SourceOrdinal, ceschema.SpecVersionOrdinal, ceschema.TypeOrdinal:
		fo = valueOffset(ordinal)
	case ceschema.DataOrdinal:
		return
	default:
		idx := doValueIndex(cb.bitmap, 1<<ordinal)
		fo = valueOffset(idx)
	}

	field := cb.buf[fo : fo+8]
	offsetAndSize := uint64(cb.nextAlloc)<<offsetOffset | uint64(len(val))
	binary.LittleEndian.PutUint64(field, offsetAndSize)
	copy(cb.buf[cb.nextAlloc:], val)
	cb.nextAlloc += alignment(len(val))
}

func (cb *marshalOptAttrCallback) OnUint16(ordinal int, val uint16) {
	cb.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

func (cb *marshalOptAttrCallback) OnUint64(ordinal int, val uint64) {
	cb.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

func (cb *marshalOptAttrCallback) OnInt64(ordinal int, val int64) {
	cb.bitmap |= 1 << ordinal

	var fo int
	switch ordinal {
	case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal:
		fo = valueOffset(ordinal)
	default:
		idx := doValueIndex(cb.bitmap, 1<<ordinal)
		fo = valueOffset(idx)
	}

	field := cb.buf[fo : fo+8]
	binary.LittleEndian.PutUint64(field, uint64(val))
}

func (cb *marshalOptAttrCallback) OnTime(ordinal int, val time.Time) {
	cb.bitmap |= 1 << ordinal

	idx := doValueIndex(cb.bitmap, 1<<ordinal)
	fo := valueOffset(idx)
	field := cb.buf[fo : fo+8]
	offsetAndNano := uint64(cb.nextAlloc)<<offsetOffset | uint64(val.Nanosecond())&sizeMask
	binary.LittleEndian.PutUint64(field, offsetAndNano)
	binary.LittleEndian.PutUint64(cb.buf[cb.nextAlloc:], uint64(val.Unix()))
	cb.nextAlloc += 8
}

func (cb *marshalOptAttrCallback) OnAttribute(ordinal int, val interface{}) {
	cb.bitmap |= 1 << ordinal

	// TODO(james.yin):
	panic("not supported type")
}

type marshalExtAttrKeyCallback struct {
	buf       []byte
	i         int
	optCnt    int
	nextAlloc int
}

// Make sure marshalExtAttrCallback implements block.ExtensionAttributesCallback.
var _ block.ExtensionAttributeCallback = (*marshalExtAttrKeyCallback)(nil)

func (cb *marshalExtAttrKeyCallback) OnAttribute(attr, _ []byte) {
	fo := attrKeyOffset(valueOffset(cb.optCnt), cb.i)
	field := cb.buf[fo : fo+8]
	offsetAndSize := uint64(cb.nextAlloc)<<32 | uint64(len(attr))
	binary.LittleEndian.PutUint64(field, offsetAndSize)
	copy(cb.buf[cb.nextAlloc:], attr)
	cb.nextAlloc += alignment(len(attr))
	cb.i++
}

type marshalExtAttrValCallback struct {
	buf       []byte
	i         int
	optCnt    int
	nextAlloc int
}

// Make sure marshalValExtAttrCallback implements block.ExtensionAttributesCallback.
var _ block.ExtensionAttributeCallback = (*marshalExtAttrValCallback)(nil)

func (cb *marshalExtAttrValCallback) OnAttribute(_, val []byte) {
	fo := attrValueOffset(valueOffset(cb.optCnt), cb.i)
	field := cb.buf[fo : fo+8]
	offsetAndSize := uint64(cb.nextAlloc)<<32 | uint64(len(val))
	binary.LittleEndian.PutUint64(field, offsetAndSize)
	copy(cb.buf[cb.nextAlloc:], val)
	cb.nextAlloc += alignment(len(val))
	cb.i++
}

func (e *ceEntryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	ext, _ := entry.(block.EntryExt)
	optCnt := ext.OptionalAttributeCount()
	extCnt := ext.ExtensionAttributeCount()

	optCb := &marshalOptAttrCallback{
		buf:       buf,
		nextAlloc: vlvrOffset(optCnt, extCnt),
	}
	ext.RangeOptionalAttributes(optCb)
	binary.LittleEndian.PutUint64(buf[:8], optCb.bitmap<<16|uint64(extCnt))

	var nextAlloc int
	if extCnt != 0 {
		// Fill attribute keys.
		keyExtCb := &marshalExtAttrKeyCallback{
			buf:       buf,
			optCnt:    optCnt,
			nextAlloc: optCb.nextAlloc,
		}
		ext.RangeExtensionAttributes(keyExtCb)

		// Fill attribute values.
		valExtCb := &marshalExtAttrValCallback{
			buf:       buf,
			optCnt:    optCnt,
			nextAlloc: keyExtCb.nextAlloc,
		}
		ext.RangeExtensionAttributes(valExtCb)

		nextAlloc = valExtCb.nextAlloc
	} else {
		nextAlloc = optCb.nextAlloc
	}

	if optCb.bitmap&(1<<ceschema.DataOrdinal) != 0 {
		data := ext.GetBytes(ceschema.DataOrdinal)
		fo := valueOffset(ceschema.DataOrdinal)
		field := buf[fo : fo+8]
		offsetAndSize := uint64(nextAlloc)<<32 | uint64(len(data))
		binary.LittleEndian.PutUint64(field, offsetAndSize)
		copy(buf[nextAlloc:], data)
		nextAlloc += alignment(len(data))
	}

	return nextAlloc, 0, nil
}

func alignment(n int) int {
	return (n + alignAddition) & alignMask
}

func vlvrOffset(valCnt, attrCnt int) int {
	return attrKeyOffset(valueOffset(valCnt), attrCnt)
}
