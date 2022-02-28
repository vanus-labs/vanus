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
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrInvalidDataType = errors.New("invalid data type, *StoredEntry expected")
)

func Marshall(entity *StoredEntry) ([]byte, error) {
	d := make([]byte, 4+len(entity.Payload))
	buffer := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buffer, binary.BigEndian, entity.Length); err != nil {
		return nil, err
	}
	v := buffer.Bytes()
	copy(d[0:4], v)
	copy(d[4:], entity.Payload)
	return d, nil
}

func Unmarshall(data []byte, v interface{}) error {
	se, ok := v.(*StoredEntry)
	if !ok {
		return ErrInvalidDataType
	}
	reader := bytes.NewReader(data[0:4])
	if err := binary.Read(reader, binary.BigEndian, &(se.Length)); err != nil {
		return err
	}
	se.Payload = data[4:]
	return nil
}
