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
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

var (
	rawData = []byte{
		0x01, 0x02, 0x03,
	}
	encodedData = []byte{
		0x04, 0x76, 0xb0, 0x1b, 0x00, 0x03, 0x01, 0x01, 0x02, 0x03,
	}
	encodedData2 = []byte{
		0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x01, 0x01, 0x02, 0x03,
	}
)

func TestType(t *testing.T) {
	Convey("terminal type", t, func() {
		So(Zero.IsTerminal(), ShouldEqual, false)
		So(Full.IsTerminal(), ShouldEqual, true)
		So(First.IsTerminal(), ShouldEqual, false)
		So(Middle.IsTerminal(), ShouldEqual, false)
		So(Last.IsTerminal(), ShouldEqual, true)
	})

	Convey("non-terminal type", t, func() {
		So(Zero.IsNonTerminal(), ShouldEqual, false)
		So(Full.IsNonTerminal(), ShouldEqual, false)
		So(First.IsNonTerminal(), ShouldEqual, true)
		So(Middle.IsNonTerminal(), ShouldEqual, true)
		So(Last.IsNonTerminal(), ShouldEqual, false)
	})
}

func TestRecord_Size(t *testing.T) {
	Convey("size", t, func() {
		r := Record{
			Data: rawData,
		}
		So(r.Size(), ShouldEqual, 4+2+1+len(rawData))
	})
}

func TestRecord_MashalTo(t *testing.T) {
	Convey("mashal record to buffer", t, func() {
		r := Record{
			Length: uint16(len(rawData)),
			Type:   Full,
			Data:   rawData,
		}
		data := make([]byte, r.Size())
		n, err := r.MarshalTo(data)
		So(err, ShouldBeNil)
		So(n, ShouldEqual, r.Size())
		So(data, ShouldResemble, encodedData)
	})
	Convey("mashal record to buffer that don't have enough space", t, func() {
		r := Record{
			Length: uint16(len(rawData)),
			Type:   Full,
			Data:   rawData,
		}
		data := make([]byte, r.Size()-1)
		_, err := r.MarshalTo(data)
		So(err, ShouldNotBeNil)
	})
}

func TestRecord_Mashal(t *testing.T) {
	Convey("mashal record", t, func() {
		r := Record{
			Length: uint16(len(rawData)),
			Type:   Full,
			Data:   rawData,
		}
		data := r.Marshal()
		So(data, ShouldResemble, encodedData)
	})
	Convey("mashal record with CRC", t, func() {
		r := Record{
			CRC:    0x00000001,
			Length: uint16(len(rawData)),
			Type:   Full,
			Data:   rawData,
		}
		data := r.Marshal()
		So(data, ShouldResemble, encodedData2)
	})
}

func TestRecord_Unmashal(t *testing.T) {
	Convey("unmashal record", t, func() {
		r, err := Unmashal(encodedData)
		So(err, ShouldBeNil)
		So(r.CRC, ShouldEqual, 0x0476b01b)
		So(r.Length, ShouldEqual, 3)
		So(r.Type, ShouldEqual, Full)
		So(r.Data, ShouldResemble, rawData)
	})

	Convey("unmashal padding space", t, func() {
		r, err := Unmashal([]byte{0x00, 0x00, 0x00, 0x00, 0x00})
		So(err, ShouldBeNil)
		So(r.CRC, ShouldEqual, 0)
		So(r.Length, ShouldEqual, 0)
		So(r.Type, ShouldEqual, Zero)
		So(r.Data, ShouldBeNil)
	})
}
