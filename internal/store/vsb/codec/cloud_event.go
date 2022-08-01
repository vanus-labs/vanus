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
)

type ceEntryEncoder struct{}

// Make sure ceEntryEncoder implements RecordDataEncoder.
var _ RecordDataEncoder = (*ceEntryEncoder)(nil)

func (e *ceEntryEncoder) Size(entry block.Entry) int {
	ext, _ := entry.(block.EntryExt)

	sz := 8 // ext count + non-null bitmap

	ext.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		sz += 8
		switch ordinal {
		case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal:
		case ceschema.TimeOrdinal:
			sz += 8
		default:
			switch v := val.(type) {
			case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			case []byte:
				sz += alignment(len(v))
			case string:
				sz += alignment(len(v))
			case time.Time:
				sz += 8
			}
		}
	})

	ext.RangeExtensionAttributes(func(attr, val []byte) {
		sz += 16 + alignment(len(attr)) + alignment(len(val))
	})

	return sz
}

func (e *ceEntryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	ext, _ := entry.(block.EntryExt)
	optCnt := ext.OptionalAttributeCount()
	extCnt := ext.ExtensionAttributeCount()
	nextAlloc := vlvrOffset(optCnt, extCnt)

	var bitmap uint64
	ext.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		bitmap |= 1 << ordinal
		switch ordinal {
		case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal,
			ceschema.IDOrdinal, ceschema.SourceOrdinal, ceschema.SpecVersionOrdinal, ceschema.TypeOrdinal:
			fo := valueOffset(ordinal)
			field := buf[fo : fo+8]
			switch ordinal {
			case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal:
				binary.LittleEndian.PutUint64(field, uint64(val.(int64)))
			case ceschema.IDOrdinal, ceschema.SourceOrdinal,
				ceschema.SpecVersionOrdinal, ceschema.TypeOrdinal:
				offsetAndSize := uint64(nextAlloc)<<offsetOffset | uint64(len(val.(string)))
				binary.LittleEndian.PutUint64(field, offsetAndSize)
				copy(buf[nextAlloc:], val.(string))
				nextAlloc += alignment(len(val.(string)))
			}
		}
	})
	binary.LittleEndian.PutUint64(buf[:8], bitmap<<16|uint64(extCnt))
	ext.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		switch ordinal {
		case ceschema.SequenceNumberOrdinal, ceschema.StimeOrdinal,
			ceschema.IDOrdinal, ceschema.SourceOrdinal, ceschema.SpecVersionOrdinal,
			ceschema.TypeOrdinal, ceschema.DataOrdinal:
		default:
			idx := doValueIndex(bitmap, 1<<ordinal)
			fo := valueOffset(idx)
			field := buf[fo : fo+8]
			switch v := val.(type) {
			case string:
				offsetAndSize := uint64(nextAlloc)<<offsetOffset | uint64(len(v))
				binary.LittleEndian.PutUint64(field, offsetAndSize)
				copy(buf[nextAlloc:], v)
				nextAlloc += alignment(len(v))
			case time.Time:
				offsetAndNano := uint64(nextAlloc)<<offsetOffset | uint64(v.Nanosecond())&sizeMask
				binary.LittleEndian.PutUint64(field, offsetAndNano)
				binary.LittleEndian.PutUint64(buf[nextAlloc:], uint64(v.Unix()))
				nextAlloc += 8
			}
		}
	})

	// Fill attribute keys.
	var i int
	ext.RangeExtensionAttributes(func(attr, _ []byte) {
		fo := attrKeyOffset(valueOffset(optCnt), i)
		field := buf[fo : fo+8]
		offsetAndSize := uint64(nextAlloc)<<32 | uint64(len(attr))
		binary.LittleEndian.PutUint64(field, offsetAndSize)
		copy(buf[nextAlloc:], attr)
		nextAlloc += alignment(len(attr))
		i++
	})
	// Fill attribute values.
	i = 0
	ext.RangeExtensionAttributes(func(_, val []byte) {
		fo := attrValueOffset(valueOffset(optCnt), i)
		field := buf[fo : fo+8]
		offsetAndSize := uint64(nextAlloc)<<32 | uint64(len(val))
		binary.LittleEndian.PutUint64(field, offsetAndSize)
		copy(buf[nextAlloc:], val)
		nextAlloc += alignment(len(val))
		i++
	})

	if bitmap&(1<<ceschema.DataOrdinal) != 0 {
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
