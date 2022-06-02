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
	"encoding/binary"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestEntry_Size(t *testing.T) {
	Convey("Get size of entry.", t, func() {
		payload := []byte("vanus")
		entry := Entry{
			Payload: payload,
		}
		sz := entry.Size()
		So(sz, ShouldEqual, EntryLengthSize+len(payload))
	})
}

func TestEntry_EndOffset(t *testing.T) {
	Convey("Get end offset of entry.", t, func() {
		payload := []byte("vanus")
		entry := Entry{
			Offset:  4096,
			Index:   0,
			Payload: payload,
		}
		eo := entry.EndOffset()
		So(eo, ShouldEqual, entry.EndOffset())
	})
}

func TestEntry_MarshalTo(t *testing.T) {
	Convey("Marshal entry", t, func() {
		payload := []byte("vanus")
		e := &Entry{
			Payload: payload,
		}
		sz := uint32FieldSize + len(payload)
		data := make([]byte, sz)
		n, err := e.MarshalTo(data)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, sz)
		entryLengthBuf := make([]byte, uint32FieldSize)
		binary.BigEndian.PutUint32(entryLengthBuf, uint32(len(payload)))
		So(data[:4], ShouldResemble, entryLengthBuf)
		So(data[4:], ShouldResemble, payload)
	})
}

func TestEntryEndOffset(t *testing.T) {
	Convey("Get end offset of entry by data.", t, func() {
		entry := Entry{
			Offset:  4096,
			Index:   0,
			Payload: []byte("vanus"),
		}
		data := entry.MarshalWithOffsetAndIndex()
		eo := EntryEndOffset(data)
		So(eo, ShouldEqual, entry.EndOffset())
	})
}
