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
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIsSpace(t *testing.T) {
	Convey("test is space", t, func() {
		So(IsSpace(' '), ShouldBeTrue)
		So(IsSpace('\t'), ShouldBeTrue)
		So(IsSpace('\n'), ShouldBeTrue)
		So(IsSpace('\r'), ShouldBeTrue)
		So(IsSpace('\\'), ShouldBeFalse)
	})
}

func TestGetIdByAddr(t *testing.T) {
	Convey("test get id by addr", t, func() {
		So(GetIDByAddr("test"), ShouldEqual, "098f6bcd4621d373cade4e832627b4f6")
	})
}
