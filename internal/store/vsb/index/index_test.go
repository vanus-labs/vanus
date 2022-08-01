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
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
	"github.com/linkall-labs/vanus/internal/store/vsb/index"
	idxtest "github.com/linkall-labs/vanus/internal/store/vsb/index/testing"
	vsbtest "github.com/linkall-labs/vanus/internal/store/vsb/testing"
)

func TestIndex(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()

	entry0 := cetest.MakeStoredEntry0(ctrl)

	Convey("vsb index", t, func() {
		idx0 := index.NewIndex(vsbtest.EntryOffset0, vsbtest.EntrySize0, index.WithEntry(entry0))
		idxtest.CheckIndex0(idx0, false)

		idx1 := index.NewIndex(vsbtest.EntryOffset1, vsbtest.EntrySize1, index.WithStime(cetest.Stime))
		idxtest.CheckIndex1(idx1, false)
	})
}
