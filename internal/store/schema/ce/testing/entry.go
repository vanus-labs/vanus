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
	"github.com/linkall-labs/vanus/internal/store/block"
	blktest "github.com/linkall-labs/vanus/internal/store/block/testing"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
)

func MakeEntry0(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(4)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(ceschema.IDOrdinal, ceID0)
		f(ceschema.SourceOrdinal, ceSource)
		f(ceschema.SpecVersionOrdinal, ceSpecVersion)
		f(ceschema.TypeOrdinal, ceType)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(0)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().Return()
	return entry
}

func MakeEntry1(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(8)
	entry.EXPECT().GetBytes(ceschema.DataOrdinal).AnyTimes().Return(ceData)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(ceschema.IDOrdinal, ceID1)
		f(ceschema.SourceOrdinal, ceSource)
		f(ceschema.SpecVersionOrdinal, ceSpecVersion)
		f(ceschema.TypeOrdinal, ceType)
		f(ceschema.DataOrdinal, ceData)
		f(ceschema.DataContentTypeOrdinal, ceDataContentType)
		f(ceschema.SubjectOrdinal, ceSubject)
		f(ceschema.TimeOrdinal, ceTime)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(3)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().DoAndReturn(func(f func(attr, val []byte)) {
		f([]byte("attr0"), []byte("value0"))
		f([]byte("attr1"), []byte("value1"))
		f([]byte("attr2"), []byte("value2"))
	})
	return entry
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
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(ceschema.SequenceNumberOrdinal, seq0)
		f(ceschema.StimeOrdinal, Stime)
		f(ceschema.IDOrdinal, ceID0)
		f(ceschema.SourceOrdinal, ceSource)
		f(ceschema.SpecVersionOrdinal, ceSpecVersion)
		f(ceschema.TypeOrdinal, ceType)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(0)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().Return()
	return entry
}

func MakeStoredEntry1(ctrl *Controller) block.EntryExt {
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
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(ceschema.SequenceNumberOrdinal, seq1)
		f(ceschema.StimeOrdinal, Stime)
		f(ceschema.IDOrdinal, ceID1)
		f(ceschema.SourceOrdinal, ceSource)
		f(ceschema.SpecVersionOrdinal, ceSpecVersion)
		f(ceschema.TypeOrdinal, ceType)
		f(ceschema.DataOrdinal, ceData)
		f(ceschema.DataContentTypeOrdinal, ceDataContentType)
		f(ceschema.SubjectOrdinal, ceSubject)
		f(ceschema.TimeOrdinal, ceTime)
	})
	entry.EXPECT().ExtensionAttributeCount().AnyTimes().Return(3)
	entry.EXPECT().RangeExtensionAttributes(Any()).AnyTimes().DoAndReturn(func(f func(attr, val []byte)) {
		f([]byte("attr0"), []byte("value0"))
		f([]byte("attr1"), []byte("value1"))
		f([]byte("attr2"), []byte("value2"))
	})
	return entry
}

func MakeStoredEndEntry(ctrl *Controller) block.EntryExt {
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(2)
	entry.EXPECT().GetUint16(ceschema.EntryTypeOrdinal).AnyTimes().Return(ceschema.End)
	entry.EXPECT().GetInt64(ceschema.SequenceNumberOrdinal).AnyTimes().Return(seq2)
	entry.EXPECT().GetInt64(ceschema.StimeOrdinal).AnyTimes().Return(Stime)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(ceschema.SequenceNumberOrdinal, seq2)
		f(ceschema.StimeOrdinal, Stime)
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
	entry.RangeExtensionAttributes(func(attr, val []byte) {
		So(false, ShouldBeTrue)
	})
}

func CheckEntryExt0(entry block.EntryExt) {
	CheckEntry0(entry, true, true)

	So(entry.OptionalAttributeCount(), ShouldEqual, 4)

	ord := 0
	entry.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		So(ordinal, ShouldBeGreaterThan, ord)
		ord = ordinal

		switch ordinal {
		case ceschema.IDOrdinal:
			So(val, ShouldEqual, ceID0)
		case ceschema.SourceOrdinal:
			So(val, ShouldEqual, ceSource)
		case ceschema.SpecVersionOrdinal:
			So(val, ShouldEqual, ceSpecVersion)
		case ceschema.TypeOrdinal:
			So(val, ShouldEqual, ceType)
		default:
			So(false, ShouldBeTrue)
		}
	})

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
	So(entry.GetExtensionAttribute([]byte("attr0")), ShouldResemble, []byte("value0"))
	So(entry.GetExtensionAttribute([]byte("attr1")), ShouldResemble, []byte("value1"))
	So(entry.GetExtensionAttribute([]byte("attr2")), ShouldResemble, []byte("value2"))

	last := ""
	entry.RangeExtensionAttributes(func(attr, val []byte) {
		str := string(attr)
		So(str, ShouldBeGreaterThan, last)
		last = str

		switch str {
		case "attr0":
			So(string(val), ShouldEqual, "value0")
		case "attr1":
			So(string(val), ShouldEqual, "value1")
		case "attr2":
			So(string(val), ShouldEqual, "value2")
		default:
			So(false, ShouldBeTrue)
		}
	})
}

func CheckEntryExt1(entry block.EntryExt) {
	CheckEntry1(entry, true, true)

	So(entry.OptionalAttributeCount(), ShouldEqual, 8)

	ord := 0
	entry.RangeOptionalAttributes(func(ordinal int, val interface{}) {
		So(ordinal, ShouldBeGreaterThan, ord)
		ord = ordinal

		switch ordinal {
		case ceschema.IDOrdinal:
			So(val, ShouldEqual, ceID1)
		case ceschema.SourceOrdinal:
			So(val, ShouldEqual, ceSource)
		case ceschema.SpecVersionOrdinal:
			So(val, ShouldEqual, ceSpecVersion)
		case ceschema.TypeOrdinal:
			So(val, ShouldEqual, ceType)
		case ceschema.DataOrdinal:
			So(val, ShouldResemble, ceData)
		case ceschema.DataContentTypeOrdinal:
			So(val, ShouldEqual, ceDataContentType)
		case ceschema.SubjectOrdinal:
			So(val, ShouldEqual, ceSubject)
		case ceschema.TimeOrdinal:
			So(val, ShouldEqual, ceTime)
		default:
			So(false, ShouldBeTrue)
		}
	})

	So(entry.ExtensionAttributeCount(), ShouldEqual, 3)
}

func CheckEndEntry(entry block.Entry, ignoreStime bool) {
	So(entry.GetInt64(ceschema.SequenceNumberOrdinal), ShouldEqual, seq2)
	if !ignoreStime {
		So(entry.GetInt64(ceschema.StimeOrdinal), ShouldEqual, Stime)
	}
}
