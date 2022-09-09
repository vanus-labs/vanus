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

package util

import (
	"fmt"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMapLen(t *testing.T) {
	Convey("test map len", t, func() {
		So(MapLen(nil), ShouldEqual, 0)
		m := &sync.Map{}
		So(MapLen(m), ShouldEqual, 0)
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("k%d", i), "v")
			So(MapLen(m), ShouldEqual, i+1)
		}
	})
}
