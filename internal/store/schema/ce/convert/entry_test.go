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
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	cetest "github.com/linkall-labs/vanus/internal/store/schema/ce/testing"
)

func TestToEntry(t *testing.T) {
	event0 := cetest.MakeEvent0()
	event1 := cetest.MakeEvent1()

	Convey("pb to entry", t, func() {
		entry0 := ToEntry(event0)
		cetest.CheckEntryExt0(entry0)

		entry1 := ToEntry(event1)
		cetest.CheckEntryExt1(entry1)
	})
}
