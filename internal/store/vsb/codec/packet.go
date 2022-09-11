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
	"hash/crc32"
	"io"

	// first-party project.
	errutil "github.com/linkall-labs/vanus/pkg/util/errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
)

const (
	packetLengthSize = 4
	packetCRCSize    = 4

	packetFooterSize = packetLengthSize + packetCRCSize
	packetMetaSize   = packetLengthSize + packetFooterSize

	packetLengthOffset  = 0
	packetPayloadOffset = packetLengthOffset + packetLengthSize
	packetCRCOffset     = packetLengthSize
)

var crc32q = crc32.MakeTable(crc32.Castagnoli)

type PacketDataEncoder interface {
	Size(entry block.Entry) int
	MarshalTo(entry block.Entry, buf []byte) (int, error)
}

type packetEncoder struct {
	pde PacketDataEncoder
}

// Make sure packetEncoder implements EntryEncoder.
var _ EntryEncoder = (*packetEncoder)(nil)

func (e *packetEncoder) Size(entry block.Entry) int {
	return packetMetaSize + e.pde.Size(entry)
}

func (e *packetEncoder) MarshalTo(entry block.Entry, buf []byte) (int, error) {
	n, err := e.pde.MarshalTo(entry, buf[packetPayloadOffset:])
	if err != nil {
		return 0, err
	}
	length := packetMetaSize + n
	footerOffset := packetPayloadOffset + n
	binary.LittleEndian.PutUint32(buf[packetLengthOffset:], uint32(length))
	binary.LittleEndian.PutUint32(buf[footerOffset:], uint32(length))
	binary.LittleEndian.PutUint32(buf[footerOffset+packetCRCOffset:],
		crc32.Checksum(buf[:footerOffset+packetCRCOffset], crc32q))
	return length, nil
}

type PacketDataDecoder interface {
	Unmarshal(data []byte) (block.Entry, error)
}

type packetDecoder struct {
	pdd      PacketDataDecoder
	checkCRC bool
}

var _ EntryDecoder = (*packetDecoder)(nil)

func (pd *packetDecoder) Unmarshal(data []byte) (int, block.Entry, error) {
	if len(data) < packetLengthSize {
		return -1, nil, ErrIncompletePacket
	}

	length := int(binary.LittleEndian.Uint32(data[packetLengthOffset:]))
	if length < packetMetaSize {
		return -1, nil, ErrCorruptedPacket
	}
	if len(data) < length {
		return -1, nil, ErrIncompletePacket
	}

	return pd.doUnmarshal(length, data[:length])
}

func (pd *packetDecoder) UnmarshalLast(data []byte) (int, block.Entry, error) {
	sz := len(data)
	if sz < packetFooterSize {
		return -1, nil, ErrIncompletePacket
	}

	length := int(binary.LittleEndian.Uint32(data[sz-packetFooterSize:]))
	if length < packetMetaSize {
		return -1, nil, ErrCorruptedPacket
	}
	if len(data) < length {
		return -1, nil, ErrIncompletePacket
	}

	return pd.doUnmarshal(length, data[sz-length:])
}

func (pd *packetDecoder) UnmarshalReader(r io.ReadSeeker) (int, block.Entry, error) {
	var lb [packetLengthSize]byte
	if _, err := r.Read(lb[:]); err != nil {
		return -1, nil, errutil.Chain(ErrIncompletePacket, err)
	}

	length := int(binary.LittleEndian.Uint32(lb[:]))
	if length < packetMetaSize {
		if length == 0 {
			return -1, nil, ErrIncompletePacket
		}
		return -1, nil, ErrCorruptedPacket
	}

	// Seek to footer.
	if _, err := r.Seek(int64(length-packetMetaSize), io.SeekCurrent); err != nil {
		return -1, nil, ErrCorruptedPacket
	}

	// Read length in footer, and check if it matches.
	if _, err := r.Read(lb[:]); err != nil {
		return -1, nil, ErrCorruptedPacket
	}
	length2 := int(binary.LittleEndian.Uint32(lb[:]))
	if length2 != length {
		return -1, nil, ErrCorruptedPacket
	}

	// Seek to start.
	if _, err := r.Seek(-int64(length-packetCRCSize), io.SeekCurrent); err != nil {
		return -1, nil, ErrCorruptedPacket
	}

	buf := make([]byte, length)
	if _, err := r.Read(buf); err != nil {
		return -1, nil, errutil.Chain(ErrIncompletePacket, err)
	}

	// FIXME(james.yin): persisted data is partial.
	return pd.doUnmarshal(length, buf)
}

func (pd *packetDecoder) doUnmarshal(length int, data []byte) (int, block.Entry, error) {
	if pd.checkCRC {
		crc := binary.LittleEndian.Uint32(data[length-packetCRCSize:])
		if crc32.Checksum(data[:length-packetCRCSize], crc32q) != crc {
			return -1, nil, ErrCorruptedPacket
		}
	}

	entry, err := pd.pdd.Unmarshal(data[packetPayloadOffset : length-packetFooterSize])
	if err != nil {
		return -1, nil, err
	}

	return length, entry, nil
}
