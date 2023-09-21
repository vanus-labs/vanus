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

package testing

import (
	// standard libraries.
	"time"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/server/store/block"
	blktest "github.com/vanus-labs/vanus/server/store/block/testing"
	ceschema "github.com/vanus-labs/vanus/server/store/schema/ce"
	cetype "github.com/vanus-labs/vanus/server/store/schema/ce/typesystem"
)

var (
	value0 = []byte{0x01}
	value1 = []byte{0x02}
	value2 = []byte{0x78, 0x56, 0x34, 0x12, 0x03}
	value3 = []byte("value3\x04")
	value4 = []byte("value4\x05")
	value5 = []byte("value5\x06")
	value6 = []byte("value6\x07")
	value7 = []byte{
		0x2E, 0xE3, 0x06, 0x63, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x08,
	}
)

func MakeEntry0(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(4)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.OptionalAttributeCallback) {
		cb.OnString(ceschema.IDOrdinal, ceID0)
		cb.OnString(ceschema.SourceOrdinal, ceSource)
		cb.OnString(ceschema.SpecVersionOrdinal, ceSpecVersion)
		cb.OnString(ceschema.TypeOrdinal, ceType)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(0)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().Return()
	return entry
}

func MakeEntry1(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(8)
	entry.EXPECT().GetBytes(ceschema.DataOrdinal).AnyTimes().Return(ceData)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.OptionalAttributeCallback) {
		cb.OnString(ceschema.IDOrdinal, ceID1)
		cb.OnString(ceschema.SourceOrdinal, ceSource)
		cb.OnString(ceschema.SpecVersionOrdinal, ceSpecVersion)
		cb.OnString(ceschema.TypeOrdinal, ceType)
		cb.OnBytes(ceschema.DataOrdinal, ceData)
		cb.OnString(ceschema.DataContentTypeOrdinal, ceDataContentType)
		cb.OnString(ceschema.SubjectOrdinal, ceSubject)
		cb.OnTime(ceschema.TimeOrdinal, ceTime)
	})
	setEntry1Extension(entry, true)
	return entry
}

func setEntry1Extension(entry *blktest.MockEntryExt, raw bool) {
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(8)
	if raw {
		entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.ExtensionAttributeCallback) {
			cb.OnAttribute([]byte("attr0"), block.BytesValue(value0))
			cb.OnAttribute([]byte("attr1"), block.BytesValue(value1))
			cb.OnAttribute([]byte("attr2"), block.BytesValue(value2))
			cb.OnAttribute([]byte("attr3"), block.BytesValue(value3))
			cb.OnAttribute([]byte("attr4"), block.BytesValue(value4))
			cb.OnAttribute([]byte("attr5"), block.BytesValue(value5))
			cb.OnAttribute([]byte("attr6"), block.BytesValue(value6))
			cb.OnAttribute([]byte("attr7"), block.BytesValue(value7))
		})
	} else {
		entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.ExtensionAttributeCallback) {
			cb.OnAttribute([]byte("attr0"), cetype.NewFalseValue())
			cb.OnAttribute([]byte("attr1"), cetype.NewTrueValue())
			cb.OnAttribute([]byte("attr2"), cetype.NewIntegerValue(0x12345678))
			cb.OnAttribute([]byte("attr3"), cetype.NewStringValue("value3"))
			cb.OnAttribute([]byte("attr4"), cetype.NewBytesValue([]byte("value4")))
			cb.OnAttribute([]byte("attr5"), cetype.NewURIValue("value5"))
			cb.OnAttribute([]byte("attr6"), cetype.NewURIRefValue("value6"))
			cb.OnAttribute([]byte("attr7"), cetype.NewTimestampValue(0x6306E32E, 0x04030201))
		})
	}
}

func MakeStoredEntry0(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(6)
	entry.EXPECT().GetUint16(ceschema.EntryTypeOrdinal).AnyTimes().Return(ceschema.CloudEvent)
	entry.EXPECT().GetInt64(ceschema.SequenceNumberOrdinal).AnyTimes().Return(seq0)
	entry.EXPECT().GetInt64(ceschema.StimeOrdinal).AnyTimes().Return(Stime)
	entry.EXPECT().GetString(ceschema.IDOrdinal).AnyTimes().Return(ceID0)
	entry.EXPECT().GetString(ceschema.SourceOrdinal).AnyTimes().Return(ceSource)
	entry.EXPECT().GetString(ceschema.SpecVersionOrdinal).AnyTimes().Return(ceSpecVersion)
	entry.EXPECT().GetString(ceschema.TypeOrdinal).AnyTimes().Return(ceType)
	entry.EXPECT().GetBytes(ceschema.DataOrdinal).AnyTimes().Return(nil)
	entry.EXPECT().GetString(ceschema.DataContentTypeOrdinal).AnyTimes().Return("")
	entry.EXPECT().GetString(ceschema.DataSchemaOrdinal).AnyTimes().Return("")
	entry.EXPECT().GetString(ceschema.SubjectOrdinal).AnyTimes().Return("")
	entry.EXPECT().GetTime(ceschema.TimeOrdinal).AnyTimes().Return(time.Time{})
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.OptionalAttributeCallback) {
		cb.OnInt64(ceschema.SequenceNumberOrdinal, seq0)
		cb.OnInt64(ceschema.StimeOrdinal, Stime)
		cb.OnString(ceschema.IDOrdinal, ceID0)
		cb.OnString(ceschema.SourceOrdinal, ceSource)
		cb.OnString(ceschema.SpecVersionOrdinal, ceSpecVersion)
		cb.OnString(ceschema.TypeOrdinal, ceType)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(0)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().Return()
	return entry
}

func MakeStoredEntry1(ctrl *Controller, raw bool) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(10)
	entry.EXPECT().GetUint16(ceschema.EntryTypeOrdinal).AnyTimes().Return(ceschema.CloudEvent)
	entry.EXPECT().GetBytes(ceschema.DataOrdinal).AnyTimes().Return(ceData)
	entry.EXPECT().GetInt64(ceschema.SequenceNumberOrdinal).AnyTimes().Return(seq1)
	entry.EXPECT().GetInt64(ceschema.StimeOrdinal).AnyTimes().Return(Stime)
	entry.EXPECT().GetString(ceschema.IDOrdinal).AnyTimes().Return(ceID1)
	entry.EXPECT().GetString(ceschema.SourceOrdinal).AnyTimes().Return(ceSource)
	entry.EXPECT().GetString(ceschema.SpecVersionOrdinal).AnyTimes().Return(ceSpecVersion)
	entry.EXPECT().GetString(ceschema.TypeOrdinal).AnyTimes().Return(ceType)
	entry.EXPECT().GetBytes(ceschema.DataOrdinal).AnyTimes().Return(ceData)
	entry.EXPECT().GetString(ceschema.DataContentTypeOrdinal).AnyTimes().Return(ceDataContentType)
	entry.EXPECT().GetString(ceschema.DataSchemaOrdinal).AnyTimes().Return("")
	entry.EXPECT().GetString(ceschema.SubjectOrdinal).AnyTimes().Return(ceSubject)
	entry.EXPECT().GetTime(ceschema.TimeOrdinal).AnyTimes().Return(ceTime)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.OptionalAttributeCallback) {
		cb.OnInt64(ceschema.SequenceNumberOrdinal, seq1)
		cb.OnInt64(ceschema.StimeOrdinal, Stime)
		cb.OnString(ceschema.IDOrdinal, ceID1)
		cb.OnString(ceschema.SourceOrdinal, ceSource)
		cb.OnString(ceschema.SpecVersionOrdinal, ceSpecVersion)
		cb.OnString(ceschema.TypeOrdinal, ceType)
		cb.OnBytes(ceschema.DataOrdinal, ceData)
		cb.OnString(ceschema.DataContentTypeOrdinal, ceDataContentType)
		cb.OnString(ceschema.SubjectOrdinal, ceSubject)
		cb.OnTime(ceschema.TimeOrdinal, ceTime)
	})
	setEntry1Extension(entry, raw)
	return entry
}

func MakeStoredEndEntry(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(2)
	entry.EXPECT().GetUint16(ceschema.EntryTypeOrdinal).AnyTimes().Return(ceschema.End)
	entry.EXPECT().GetInt64(ceschema.SequenceNumberOrdinal).AnyTimes().Return(seq2)
	entry.EXPECT().GetInt64(ceschema.StimeOrdinal).AnyTimes().Return(Stime)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(cb block.OptionalAttributeCallback) {
		cb.OnInt64(ceschema.SequenceNumberOrdinal, seq2)
		cb.OnInt64(ceschema.StimeOrdinal, Stime)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(0)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().Return()
	return entry
}

func CheckEntry0(entry block.Entry, ignoreSeq, ignoreStime bool) {
	if !ignoreSeq {
		So(entry.GetInt64(ceschema.SequenceNumberOrdinal), ShouldEqual, seq0)
	}
	if !ignoreStime {
		So(entry.GetInt64(ceschema.StimeOrdinal), ShouldEqual, Stime)
	}
	So(entry.GetString(ceschema.IDOrdinal), ShouldEqual, ceID0)
	So(entry.GetString(ceschema.SourceOrdinal), ShouldEqual, ceSource)
	So(entry.GetString(ceschema.SpecVersionOrdinal), ShouldEqual, ceSpecVersion)
	So(entry.GetString(ceschema.TypeOrdinal), ShouldEqual, ceType)
	entry.RangeExtensionAttributes(block.OnExtensionAttributeFunc(func(attr []byte, val block.Value) {
		So(false, ShouldBeTrue)
	}))
}

func CheckEntryExt0(entry block.EntryExt) {
	CheckEntry0(entry, true, true)

	So(entry.OptionalAttributeCount(), ShouldEqual, 4)

	ord := 0
	entry.RangeOptionalAttributes(block.OnOptionalAttributeFunc(func(ordinal int, val interface{}) {
		So(ordinal, ShouldBeGreaterThan, ord)
		ord = ordinal

		switch ordinal {
		case ceschema.IDOrdinal:
			So(val.(string), ShouldEqual, ceID0)
		case ceschema.SourceOrdinal:
			So(val.(string), ShouldEqual, ceSource)
		case ceschema.SpecVersionOrdinal:
			So(val.(string), ShouldEqual, ceSpecVersion)
		case ceschema.TypeOrdinal:
			So(val.(string), ShouldEqual, ceType)
		default:
			So(false, ShouldBeTrue)
		}
	}))

	So(entry.ExtensionAttributeCount(), ShouldEqual, 0)
}

func CheckEntry1(entry block.Entry, ignoreSeq, ignoreStime bool) {
	if !ignoreSeq {
		So(entry.GetInt64(ceschema.SequenceNumberOrdinal), ShouldEqual, seq1)
	}
	if !ignoreStime {
		So(entry.GetInt64(ceschema.StimeOrdinal), ShouldEqual, Stime)
	}
	So(entry.GetString(ceschema.IDOrdinal), ShouldEqual, ceID1)
	So(entry.GetString(ceschema.SourceOrdinal), ShouldEqual, ceSource)
	So(entry.GetString(ceschema.SpecVersionOrdinal), ShouldEqual, ceSpecVersion)
	So(entry.GetString(ceschema.TypeOrdinal), ShouldEqual, ceType)
	So(entry.GetBytes(ceschema.DataOrdinal), ShouldResemble, ceData)
	So(entry.GetString(ceschema.DataContentTypeOrdinal), ShouldEqual, ceDataContentType)
	So(entry.GetString(ceschema.DataSchemaOrdinal), ShouldBeZeroValue)
	So(entry.GetString(ceschema.SubjectOrdinal), ShouldEqual, ceSubject)
	So(entry.GetTime(ceschema.TimeOrdinal), ShouldEqual, ceTime)
	So(entry.GetExtensionAttribute([]byte("attr0")), ShouldResemble, value0)
	So(entry.GetExtensionAttribute([]byte("attr1")), ShouldResemble, value1)
	So(entry.GetExtensionAttribute([]byte("attr2")), ShouldResemble, value2)
	So(entry.GetExtensionAttribute([]byte("attr3")), ShouldResemble, value3)
	So(entry.GetExtensionAttribute([]byte("attr4")), ShouldResemble, value4)
	So(entry.GetExtensionAttribute([]byte("attr5")), ShouldResemble, value5)
	So(entry.GetExtensionAttribute([]byte("attr6")), ShouldResemble, value6)
	So(entry.GetExtensionAttribute([]byte("attr7")), ShouldResemble, value7)

	last := ""
	entry.RangeExtensionAttributes(block.OnExtensionAttributeFunc(func(attr []byte, val block.Value) {
		str := string(attr)
		So(str, ShouldBeGreaterThan, last)
		last = str

		switch str {
		case "attr0":
			So(val.Size(), ShouldEqual, len(value0))
			So(val.Value(), ShouldResemble, value0)
		case "attr1":
			So(val.Size(), ShouldEqual, len(value1))
			So(val.Value(), ShouldResemble, value1)
		case "attr2":
			So(val.Size(), ShouldEqual, len(value2))
			So(val.Value(), ShouldResemble, value2)
		case "attr3":
			So(val.Size(), ShouldEqual, len(value3))
			So(val.Value(), ShouldResemble, value3)
		case "attr4":
			So(val.Size(), ShouldEqual, len(value4))
			So(val.Value(), ShouldResemble, value4)
		case "attr5":
			So(val.Size(), ShouldEqual, len(value5))
			So(val.Value(), ShouldResemble, value5)
		case "attr6":
			So(val.Size(), ShouldEqual, len(value6))
			So(val.Value(), ShouldResemble, value6)
		case "attr7":
			So(val.Size(), ShouldEqual, len(value7))
			So(val.Value(), ShouldResemble, value7)
		default:
			So(false, ShouldBeTrue)
		}
	}))
}

func CheckEntryExt1(entry block.EntryExt) {
	CheckEntry1(entry, true, true)

	So(entry.OptionalAttributeCount(), ShouldEqual, 8)

	ord := 0
	entry.RangeOptionalAttributes(block.OnOptionalAttributeFunc(func(ordinal int, val interface{}) {
		So(ordinal, ShouldBeGreaterThan, ord)
		ord = ordinal

		switch ordinal {
		case ceschema.IDOrdinal:
			So(val.(string), ShouldEqual, ceID1)
		case ceschema.SourceOrdinal:
			So(val.(string), ShouldEqual, ceSource)
		case ceschema.SpecVersionOrdinal:
			So(val.(string), ShouldEqual, ceSpecVersion)
		case ceschema.TypeOrdinal:
			So(val.(string), ShouldEqual, ceType)
		case ceschema.DataOrdinal:
			So(val, ShouldResemble, ceData)
		case ceschema.DataContentTypeOrdinal:
			So(val.(string), ShouldEqual, ceDataContentType)
		case ceschema.SubjectOrdinal:
			So(val.(string), ShouldEqual, ceSubject)
		case ceschema.TimeOrdinal:
			So(val, ShouldEqual, ceTime)
		default:
			So(false, ShouldBeTrue)
		}
	}))

	So(entry.ExtensionAttributeCount(), ShouldEqual, 8)
}

func CheckEndEntry(entry block.Entry, ignoreStime bool) {
	So(entry.GetInt64(ceschema.SequenceNumberOrdinal), ShouldEqual, seq2)
	if !ignoreStime {
		So(entry.GetInt64(ceschema.StimeOrdinal), ShouldEqual, Stime)
	}
}
