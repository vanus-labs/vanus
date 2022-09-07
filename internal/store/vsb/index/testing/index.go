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
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func MakeIndex0(ctrl *Controller) index.Index {
	i := NewMockIndex(ctrl)
	i.EXPECT().StartOffset().AnyTimes().Return(vsbtest.EntryOffset0)
	i.EXPECT().EndOffset().AnyTimes().Return(vsbtest.EntryOffset0 + vsbtest.EntrySize0)
	i.EXPECT().Length().AnyTimes().Return(int32(vsbtest.EntrySize0))
	i.EXPECT().Stime().AnyTimes().Return(cetest.Stime)
	return i
}

func MakeIndex1(ctrl *Controller) index.Index {
	i := NewMockIndex(ctrl)
	i.EXPECT().StartOffset().AnyTimes().Return(vsbtest.EntryOffset1)
	i.EXPECT().EndOffset().AnyTimes().Return(vsbtest.EntryOffset1 + vsbtest.EntrySize1)
	i.EXPECT().Length().AnyTimes().Return(int32(vsbtest.EntrySize1))
	i.EXPECT().Stime().AnyTimes().Return(cetest.Stime)
	return i
}

func CheckIndex0(i index.Index, ignoreStime bool) {
	So(i.StartOffset(), ShouldEqual, vsbtest.EntryOffset0)
	So(i.EndOffset(), ShouldEqual, vsbtest.EntryOffset0+vsbtest.EntrySize0)
	So(i.Length(), ShouldEqual, vsbtest.EntrySize0)
	if !ignoreStime {
		So(i.Stime(), ShouldEqual, cetest.Stime)
	}
}

func CheckIndex1(i index.Index, ignoreStime bool) {
	So(i.StartOffset(), ShouldEqual, vsbtest.EntryOffset1)
	So(i.EndOffset(), ShouldEqual, vsbtest.EntryOffset1+vsbtest.EntrySize1)
	So(i.Length(), ShouldEqual, vsbtest.EntrySize1)
	if !ignoreStime {
		So(i.Stime(), ShouldEqual, cetest.Stime)
	}
}
