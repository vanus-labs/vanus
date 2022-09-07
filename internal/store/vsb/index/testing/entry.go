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
	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/block"
	blktest "github.com/linkall-labs/vanus/internal/store/block/testing"
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
)

func MakeEntry(ctrl *Controller) block.EntryExt {
	idx0 := MakeIndex0(ctrl)
	idx1 := MakeIndex1(ctrl)
	entry := blktest.NewMockEntryExt(ctrl)
	entry.EXPECT().OptionalAttributeCount().AnyTimes().Return(2)
	entry.EXPECT().GetUint16(ceschema.EntryTypeOrdinal).AnyTimes().Return(ceschema.Index)
	entry.EXPECT().RangeOptionalAttributes(Any()).AnyTimes().DoAndReturn(func(f func(ordinal int, val interface{})) {
		f(0, idx0)
		f(1, idx1)
	})
	return entry
}

func CheckEntry(entry block.Entry, ignoreStime bool) {
	ext, ok := entry.(block.EntryExt)
	So(ok, ShouldBeTrue)
	So(ext.OptionalAttributeCount(), ShouldEqual, 2)

	indexes, ok := entry.Get(ceschema.IndexesOrdinal).([]index.Index)
	So(ok, ShouldBeTrue)
	So(indexes, ShouldHaveLength, 2)
	CheckIndex0(indexes[0], ignoreStime)
	CheckIndex1(indexes[1], ignoreStime)
}
