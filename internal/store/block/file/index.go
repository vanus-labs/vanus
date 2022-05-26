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

package file

import (
	// standard libraries.
	"bytes"
	"encoding/binary"
)

const (
	v1IndexSize = 8 + 4
)

type index struct {
	offset int64
	length int32
}

func (i index) StartOffset() int64 {
	return i.offset
}

func (i index) EndOffset() int64 {
	return i.offset + int64(i.length)
}

func (i index) MarshalTo(data []byte) (int, error) {
	if len(data) < v1IndexSize {
		// TODO(james.yin): correct error.
		return 0, bytes.ErrTooLarge
	}
	binary.BigEndian.PutUint64(data[0:8], uint64(i.offset))
	binary.BigEndian.PutUint32(data[8:12], uint32(i.length))
	return v1IndexSize, nil
}

func unmarshalIndex(data []byte) (index, error) {
	if len(data) < v1IndexSize {
		// TODO(james.yin): correct error.
		return index{}, bytes.ErrTooLarge
	}
	i := index{
		offset: int64(binary.BigEndian.Uint64(data[0:8])),
		length: int32(binary.BigEndian.Uint32(data[8:12])),
	}
	return i, nil
}
