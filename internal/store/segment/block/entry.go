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
	"bytes"
	"encoding/binary"
)

type Entry struct {
	Offset  uint32
	Index   uint32
	Payload []byte
}

// Size returns the size of space used by the entry in storage.
func (e Entry) Size() int {
	return 4 + len(e.Payload)
}

func (e Entry) MarshalWithOffsetAndIndex() []byte {
	buf := make([]byte, 4+4+len(e.Payload))
	e.doMarshalTo(buf, e.Offset, e.Index)
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
		so += 4
	}
	if len(e.Payload) > 0 {
		so += copy(data[so:], e.Payload)
	}
	return so, nil
}

func UnmarshalWithOffsetAndIndex(data []byte) (e Entry) {
	e.Offset = binary.BigEndian.Uint32(data[0:4])
	e.Index = binary.BigEndian.Uint32(data[4:8])
	e.Payload = data[8:]
	return e
}
