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

package convert

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
)

func TestToPb(t *testing.T) {
	ctrl := NewController(t)
	defer ctrl.Finish()

	entry0 := cetest.MakeStoredEntry0(ctrl)
	entry1 := cetest.MakeStoredEntry1(ctrl)

	Convey("entry to pb", t, func() {
		event0 := ToPb(entry0)
		cetest.CheckEvent0(event0)

		event1 := ToPb(entry1)
		cetest.CheckEvent1(event1)
	})
}
