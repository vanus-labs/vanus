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
	Convey("Get size of entry", t, func() {
		payload := []byte("vanus")
		entry := Entry{
			Payload: payload,
		}
		sz := entry.Size()
		So(sz, ShouldEqual, EntryLengthSize+len(payload))
	})
}

func TestEntry_EndOffset(t *testing.T) {
	Convey("Get end offset of entry", t, func() {
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

func TestEntry_MarshalWithOffsetAndIndex(t *testing.T) {
	Convey("Marshal entry with offset and index", t, func() {
		payload := []byte("vanus")
		e := &Entry{
			Offset:  4096,
			Index:   1,
			Payload: payload,
		}
		data := e.MarshalWithOffsetAndIndex()
		buf := make([]byte, uint32FieldSize)
		binary.BigEndian.PutUint32(buf, 4096)
		So(data[0:4], ShouldResemble, buf)
		binary.BigEndian.PutUint32(buf, 1)
		So(data[4:8], ShouldResemble, buf)
		So(data[8:], ShouldResemble, payload)
	})
}

func TestEntry_MarshalTo(t *testing.T) {
	Convey("Marshal entry", t, func() {
		payload := []byte("vanus")
		e := &Entry{
			Payload: payload,
		}
		sz := e.Size()

		Convey("Marshal entry to buffer", func() {
			data := make([]byte, sz)
			n, err := e.MarshalTo(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, sz)
			entryLengthBuf := make([]byte, uint32FieldSize)
			binary.BigEndian.PutUint32(entryLengthBuf, uint32(len(payload)))
			So(data[0:4], ShouldResemble, entryLengthBuf)
			So(data[4:], ShouldResemble, payload)
		})

		Convey("Marshal entry to buffer that don't have enough space", func() {
			data := make([]byte, sz-1)
			_, err := e.MarshalTo(data)
			So(err, ShouldNotBeNil)
		})

		Convey("Marshal entry to buffer that have excess space", func() {
			data := make([]byte, sz+1)
			data[sz] = 0xF0
			n, err := e.MarshalTo(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, sz)
			entryLengthBuf := make([]byte, uint32FieldSize)
			binary.BigEndian.PutUint32(entryLengthBuf, uint32(len(payload)))
			So(data[0:4], ShouldResemble, entryLengthBuf)
			So(data[4:sz], ShouldResemble, payload)
			So(data[sz], ShouldEqual, 0xF0)
		})
	})
}

func TestUnmarshalEntry(t *testing.T) {
	Convey("Unmarshal entry", t, func() {
		payload := []byte("vanus")
		entry := Entry{
			Offset:  4096,
			Index:   1,
			Payload: payload,
		}

		Convey("Unmarshal with length", func() {
			data := make([]byte, entry.Size())
			entry.MarshalTo(data)
			e, err := UnmarshalEntry(data)
			So(err, ShouldBeNil)
			So(e.Payload, ShouldResemble, payload)

			data[3]--
			e, err = UnmarshalEntry(data)
			So(err, ShouldBeNil)
			So(e.Payload, ShouldResemble, payload[:len(payload)-1])
		})

		Convey("Unmarshal with corrupted data", func() {
			data := make([]byte, entry.Size())
			entry.MarshalTo(data)

			data[3]++
			_, err := UnmarshalEntry(data)
			So(err, ShouldNotBeNil)

			_, err = UnmarshalEntry(make([]byte, EntryLengthSize-1))
			So(err, ShouldNotBeNil)
		})

		Convey("Unmarshal with offset and index", func() {
			data := entry.MarshalWithOffsetAndIndex()
			e := UnmarshalEntryWithOffsetAndIndex(data)
			So(e, ShouldResemble, entry)
		})
	})
}

func TestEntryEndOffset(t *testing.T) {
	Convey("Get end offset of entry by data", t, func() {
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

func TestEntryLength(t *testing.T) {
	Convey("Get length of entry by data", t, func() {
		payload := []byte("vanus")
		entry := Entry{
			Payload: payload,
		}
		data := make([]byte, entry.Size())
		entry.MarshalTo(data)
		length := EntryLength(data)
		So(length, ShouldEqual, len(payload))
	})
}
