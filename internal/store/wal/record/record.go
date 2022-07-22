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

package record

import (
	// standard libraries.
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

const (
	crcFieldSO    = 0
	crcFieldEO    = crcFieldSO + 4
	lengthFieldSO = crcFieldEO
	lengthFieldEO = lengthFieldSO + 2
	typeFieldSO   = lengthFieldEO
	typeFieldEO   = typeFieldSO + 1
	dataFieldSO   = typeFieldEO
)

const HeaderSize = dataFieldSO

var crc32q = crc32.MakeTable(crc32.Castagnoli)

type Type uint8

const (
	Zero Type = iota
	Full
	First
	Middle
	Last
)

func (t Type) IsTerminal() bool {
	return t == Last || t == Full
}

func (t Type) IsNonTerminal() bool {
	return t == Middle || t == First
}

type Record struct {
	// CRC is crc32c of Type and Data
	CRC uint32
	// Length is len(Data). optimize?
	Length uint16
	Type   Type
	Data   []byte
}

func (r *Record) Size() int {
	return typeFieldEO + len(r.Data)
}

func (r *Record) Marshal() []byte {
	data := make([]byte, r.Size())
	_, _ = r.MarshalTo(data)
	return data
}

func (r *Record) MarshalTo(data []byte) (int, error) {
	sz := r.Size()
	if len(data) < sz {
		// TODO(james.yin): correct error.
		return 0, bytes.ErrTooLarge
	}
	binary.BigEndian.PutUint16(data[lengthFieldSO:lengthFieldEO], r.Length)
	data[typeFieldSO] = byte(r.Type)
	ds := len(r.Data)
	if ds != 0 {
		copy(data[dataFieldSO:dataFieldSO+ds], r.Data)
	}
	// calculate CRC
	if r.CRC == 0 {
		r.CRC = crc32.Checksum(data[typeFieldSO:dataFieldSO+ds], crc32q)
	}
	binary.BigEndian.PutUint32(data[crcFieldSO:crcFieldEO], r.CRC)
	return sz, nil
}

func Unmarshal(data []byte) (record Record, err error) {
	if len(data) < HeaderSize {
		// return empty record
		return record, nil
	}
	record.CRC = binary.BigEndian.Uint32(data[crcFieldSO:crcFieldEO])
	record.Length = binary.BigEndian.Uint16(data[lengthFieldSO:lengthFieldEO])
	record.Type = Type(data[typeFieldSO])
	if len(data) < int(record.Length)+HeaderSize {
		// TODO(james.yin): correct error
		return record, bytes.ErrTooLarge
	}
	record.Data = data[dataFieldSO : dataFieldSO+record.Length]
	return record, nil
}
