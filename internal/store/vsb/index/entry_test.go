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

package index_test

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	ceschema "github.com/linkall-labs/vanus/internal/store/schema/ce"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
)

func TestEntry(t *testing.T) {
	Convey("index entry", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		ent := index.NewEntry([]index.Index{
			idxtest.MakeIndex0(ctrl),
			idxtest.MakeIndex1(ctrl),
		})

		So(ceschema.EntryType(ent), ShouldEqual, ceschema.Index)
		idxtest.CheckEntry(ent, false)

		idx0, ok0 := ent.Get(0).(index.Index)
		So(ok0, ShouldBeTrue)
		idxtest.CheckIndex0(idx0, false)

		idx1, ok1 := ent.Get(1).(index.Index)
		So(ok1, ShouldBeTrue)
		idxtest.CheckIndex1(idx1, false)

		_, ok2 := ent.Get(2).(index.Index)
		So(ok2, ShouldBeFalse)
	})
}
