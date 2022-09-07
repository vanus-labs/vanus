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
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

type indexEntryEncoder struct {
	indexSize int
}

// Make sure indexEntryEncoder implements RecordDataEncoder.
var _ RecordDataEncoder = (*indexEntryEncoder)(nil)

func (e *indexEntryEncoder) Size(entry block.Entry) int {
	ext, _ := entry.(block.EntryExt)
	return e.indexSize * ext.OptionalAttributeCount()
}

func (e *indexEntryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	ext, _ := entry.(block.EntryExt)
	var num int
	ext.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		idx, _ := val.(index.Index)
		data := buf[ordinal*e.indexSize : (ordinal+1)*e.indexSize]
		binary.LittleEndian.PutUint64(data[0:], uint64(idx.StartOffset())) // offset
		binary.LittleEndian.PutUint32(data[8:], uint32(idx.Length()))      // length
		binary.LittleEndian.PutUint32(data[12:], 0)                        // reserved
		binary.LittleEndian.PutUint64(data[16:], uint64(idx.Stime()))      // stime
		num++
	})
	return e.indexSize * num, 0, nil
}

type indexEntryDecoder struct {
	indexSize int
}

// Make sure indexEntryDecoder implements RecordDataDecoder.
var _ RecordDataDecoder = (*indexEntryDecoder)(nil)

func (d *indexEntryDecoder) Unmarshal(t uint16, offset int, data []byte) (block.Entry, error) {
	payload := data
	if offset > 0 {
		payload = data[offset:]
	}

	if len(payload)%d.indexSize != 0 {
		return nil, ErrCorruptedRecord
	}

	indexes := make([]index.Index, 0, len(payload)/d.indexSize)
	for i := 0; i < len(payload); i += d.indexSize {
		buf := payload[i : i+d.indexSize]
		offset := int64(binary.LittleEndian.Uint64(buf[0:]))
		length := int32(binary.LittleEndian.Uint32(buf[8:]))
		stime := int64(binary.LittleEndian.Uint64(buf[16:]))

		idx := index.NewIndex(offset, length, index.WithStime(stime))
		indexes = append(indexes, idx)
	}

	return index.NewEntry(indexes), nil
}
