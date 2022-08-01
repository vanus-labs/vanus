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

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

const (
	recordTypeSize   = 2
	recordOffsetSize = 2

	recordHeaderSize = recordTypeSize + recordOffsetSize

	recordTypeOffset    = 0
	recordOffsetOffset  = recordTypeOffset + recordTypeSize
	recordPayloadOffset = recordOffsetOffset + recordOffsetSize
)

type RecordDataEncoder interface {
	Size(entry block.Entry) int
	MarshalTo(entry block.Entry, buf []byte) (int, int, error)
}

type recordEncoder struct {
	rde RecordDataEncoder
}

// Make sure recordEncoder implements PacketDataEncoder.
var _ PacketDataEncoder = (*recordEncoder)(nil)

func (e *recordEncoder) Size(entry block.Entry) int {
	return recordHeaderSize + e.rde.Size(entry)
}

func (e *recordEncoder) MarshalTo(entry block.Entry, buf []byte) (int, error) {
	n, headerSize, err := e.rde.MarshalTo(entry, buf[recordPayloadOffset:])
	if err != nil {
		return -1, err
	}
	binary.LittleEndian.PutUint16(buf[recordTypeOffset:], ceschema.EntryType(entry))
	binary.LittleEndian.PutUint16(buf[recordOffsetOffset:], recordPayloadOffset+uint16(headerSize))
	return recordPayloadOffset + n, nil
}

type RecordDataDecoder interface {
	Unmarshal(t uint16, offset int, data []byte) (block.Entry, error)
}

type recordDecoder struct {
	rdd RecordDataDecoder
}

// Make sure recordDecoder implemets PacketDataDecoder.
var _ PacketDataDecoder = (*recordDecoder)(nil)

func (d *recordDecoder) Unmarshal(data []byte) (block.Entry, error) {
	t := binary.LittleEndian.Uint16(data)
	offset := binary.LittleEndian.Uint16(data[recordOffsetOffset:])
	if offset < recordPayloadOffset {
		return nil, ErrCorruptedRecord
	}
	return d.rdd.Unmarshal(t, int(offset)-recordPayloadOffset, data[recordPayloadOffset:])
}
