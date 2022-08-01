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
	"errors"
	"io"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
)

const IndexSize = 24

var (
	ErrInvalid          = errors.New("vsb.codec: invalid argument")
	ErrBufferNotEnough  = errors.New("vsb.codec: buffer not enough")
	ErrIncompletePacket = errors.New("vsb.codec: incomplete packet")
	ErrCorruptedPacket  = errors.New("vsb.codec: corrupted packet")
	ErrCorruptedRecord  = errors.New("vsb.codec: corrupted record")
	ErrUnknownRecord    = errors.New("vsb.codec: unknown record")
)

type EntryEncoder interface {
	Size(entry block.Entry) int
	MarshalTo(entry block.Entry, buf []byte) (int, error)
}

type EntryDecoder interface {
	Unmarshal(data []byte) (int, block.Entry, error)
	UnmarshalLast(data []byte) (int, block.Entry, error)
	UnmarshalReader(r io.ReadSeeker) (int, block.Entry, error)
}

func NewEncoder() EntryEncoder {
	return &packetEncoder{
		pde: &recordEncoder{
			rde: &entryEncoder{
				indexEnc: indexEntryEncoder{
					indexSize: IndexSize,
				},
			},
		},
	}
}

func NewDecoder(checkCRC bool, indexSize int) (EntryDecoder, error) {
	if indexSize < IndexSize {
		return nil, ErrInvalid
	}
	dec := &packetDecoder{
		pdd: &recordDecoder{
			rdd: &entryDecoder{
				indexDec: indexEntryDecoder{
					indexSize: indexSize,
				},
			},
		},
		checkCRC: checkCRC,
	}
	return dec, nil
}
