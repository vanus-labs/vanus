// Copyright 2023 Linkall Inc.
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

package typesystem

import (
	// standard libraries.
	"encoding/binary"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/block"
)

type AttrType = byte

const (
	AttrTypeNone      AttrType = 0
	AttrTypeFalse     AttrType = 1
	AttrTypeTrue      AttrType = 2
	AttrTypeInteger   AttrType = 3
	AttrTypeString    AttrType = 4
	AttrTypeBytes     AttrType = 5
	AttrTypeURI       AttrType = 6
	AttrTypeURIRef    AttrType = 7
	AttrTypeTimestamp AttrType = 8

	integerValueSize   = 5
	timestampValueSize = 13
)

var (
	rawNoneValue  = []byte{AttrTypeNone}
	rawFalseValue = []byte{AttrTypeFalse}
	rawTrueValue  = []byte{AttrTypeTrue}

	noneValue  = &literalValue{rawNoneValue}
	falseValue = &literalValue{rawFalseValue}
	trueValue  = &literalValue{rawTrueValue}
)

type literalValue struct {
	rawValue []byte
}

// Make sure literalValue implements block.ValueMarshaler.
var _ block.ValueMarshaler = (*literalValue)(nil)

func (lv *literalValue) Size() int {
	return len(lv.rawValue)
}

func (lv *literalValue) Value() []byte {
	return lv.rawValue
}

func (lv *literalValue) MarshalTo(buf []byte) int {
	return copy(buf, lv.rawValue)
}

type integerValue struct {
	value int32
}

// Make sure integerValue implements block.ValueMarshaler.
var _ block.ValueMarshaler = (*integerValue)(nil)

func newIntegerValue(value int32) *integerValue {
	return &integerValue{value}
}

func (iv *integerValue) Size() int {
	return integerValueSize
}

func (iv *integerValue) Value() []byte {
	b := make([]byte, integerValueSize)
	iv.MarshalTo(b)
	return b
}

func (iv *integerValue) MarshalTo(buf []byte) int {
	binary.LittleEndian.PutUint32(buf, uint32(iv.value))
	buf[4] = AttrTypeInteger
	return integerValueSize
}

type bytesValue[T string | []byte] struct {
	value    T
	attrType byte
}

// Make sure bytesValue implements block.ValueMarshaler.
var _ block.ValueMarshaler = (*bytesValue[string])(nil)

func (bv *bytesValue[T]) Size() int {
	return len(bv.value) + 1
}

func (bv *bytesValue[T]) Value() []byte {
	b := make([]byte, len(bv.value)+1)
	bv.MarshalTo(b)
	return b
}

func (bv *bytesValue[T]) MarshalTo(buf []byte) int {
	n := len(bv.value)
	copy(buf, bv.value)
	buf[n] = bv.attrType
	return n + 1
}

func newStringValue(value string) *bytesValue[string] {
	return &bytesValue[string]{value, AttrTypeString}
}

func newBytesValue(value []byte) *bytesValue[[]byte] {
	return &bytesValue[[]byte]{value, AttrTypeBytes}
}

func newURIValue(value string) *bytesValue[string] {
	return &bytesValue[string]{value, AttrTypeURI}
}

func newURIRefValue(value string) *bytesValue[string] {
	return &bytesValue[string]{value, AttrTypeURIRef}
}

type timestampValue struct {
	seconds int64
	nanos   int32
}

func newTimestampValue(seconds int64, nanos int32) *timestampValue {
	return &timestampValue{seconds, nanos}
}

// Make sure timestampValue implements block.ValueMarshaler.
var _ block.ValueMarshaler = (*timestampValue)(nil)

func (tv *timestampValue) Size() int {
	return timestampValueSize
}

func (tv *timestampValue) Value() []byte {
	b := make([]byte, timestampValueSize)
	tv.MarshalTo(b)
	return b
}

func (tv *timestampValue) MarshalTo(buf []byte) int {
	binary.LittleEndian.PutUint64(buf, uint64(tv.seconds))
	binary.LittleEndian.PutUint32(buf[8:], uint32(tv.nanos))
	buf[12] = AttrTypeTimestamp
	return timestampValueSize
}

func NewNoneValue() block.ValueMarshaler {
	return noneValue
}

func NewFalseValue() block.ValueMarshaler {
	return falseValue
}

func NewTrueValue() block.ValueMarshaler {
	return trueValue
}

func NewIntegerValue(integer int32) block.ValueMarshaler {
	return newIntegerValue(integer)
}

func NewStringValue(str string) block.ValueMarshaler {
	return newStringValue(str)
}

func NewBytesValue(bytes []byte) block.ValueMarshaler {
	return newBytesValue(bytes)
}

func NewURIValue(uri string) block.ValueMarshaler {
	return newURIValue(uri)
}

func NewURIRefValue(uriRef string) block.ValueMarshaler {
	return newURIRefValue(uriRef)
}

func NewTimestampValue(secs int64, nanos int32) block.ValueMarshaler {
	return newTimestampValue(secs, nanos)
}
