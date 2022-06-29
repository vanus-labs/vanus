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
	"bytes"
	"encoding/binary"
)

const (
	uint32FieldSize = 4
	entryOffsetSize = uint32FieldSize
	entryIndexSize  = uint32FieldSize
)

const (
	EntryLengthSize = uint32FieldSize
)

type Entry struct {
	Offset  uint32
	Index   uint32
	Payload []byte
	// TODO: CRC
}

// Size returns the size of space used by the entry in storage.
func (e Entry) Size() int {
	return EntryLengthSize + len(e.Payload)
}

func (e Entry) EndOffset() uint32 {
	return e.Offset + uint32(e.Size())
}

func (e Entry) MarshalWithOffsetAndIndex() []byte {
	buf := make([]byte, entryOffsetSize+entryIndexSize+len(e.Payload))
	_, _ = e.doMarshalTo(buf, e.Offset, e.Index)
	return buf
}

func (e Entry) MarshalTo(data []byte) (int, error) {
	sz := e.Size()
	if len(data) < sz {
		// TODO(james.yin): correct error.
		return 0, bytes.ErrTooLarge
	}
	return e.doMarshalTo(data, uint32(len(e.Payload)))
}

func (e Entry) doMarshalTo(data []byte, values ...uint32) (int, error) {
	so := 0
	for _, value := range values {
		binary.BigEndian.PutUint32(data[so:], value)
		so += uint32FieldSize
	}
	if len(e.Payload) != 0 {
		so += copy(data[so:], e.Payload)
	}
	return so, nil
}

func UnmarshalEntryWithOffsetAndIndex(data []byte) (e Entry) {
	e.Offset = binary.BigEndian.Uint32(data[0:entryOffsetSize])
	e.Index = binary.BigEndian.Uint32(data[entryOffsetSize : entryOffsetSize+entryIndexSize])
	e.Payload = data[entryOffsetSize+entryIndexSize:]
	return e
}

func UnmarshalEntry(data []byte) (Entry, error) {
	sz := uint32(len(data))
	if sz < EntryLengthSize {
		return Entry{}, bytes.ErrTooLarge
	}
	length := binary.BigEndian.Uint32(data[0:EntryLengthSize])
	if sz-EntryLengthSize < length {
		return Entry{}, bytes.ErrTooLarge
	}
	e := Entry{
		Payload: data[EntryLengthSize : EntryLengthSize+length],
	}
	return e, nil
}

func EntryEndOffset(data []byte) uint32 {
	offset := binary.BigEndian.Uint32(data[0:entryOffsetSize])
	return offset + EntryLengthSize + uint32(len(data[entryOffsetSize+entryIndexSize:]))
}

func EntryLength(data []byte) uint32 {
	return binary.BigEndian.Uint32(data[0:EntryLengthSize])
}
