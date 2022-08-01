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
	endEntrySize = 8 + 8 + 8

	endBitmap uint64 = 0b11
)

type endEntryEncoder struct{}

// Make sure endEntryEncoder implements RecordDataEncoder.
var _ RecordDataEncoder = (*endEntryEncoder)(nil)

func (e *endEntryEncoder) Size(entry block.Entry) int {
	return endEntrySize
}

func (e *endEntryEncoder) MarshalTo(entry block.Entry, buf []byte) (int, int, error) {
	binary.LittleEndian.PutUint64(buf[0:], endBitmap<<bitmapOffset)                // bitmap
	binary.LittleEndian.PutUint64(buf[8:], uint64(ceschema.SequenceNumber(entry))) // seq num
	binary.LittleEndian.PutUint64(buf[16:], uint64(ceschema.Stime(entry)))         // stime
	return endEntrySize, 0, nil
}
