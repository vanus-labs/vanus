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

func TestIsValidIPV4Address(t *testing.T) {
	Convey("test is valid ipv4 address", t, func() {
		valid := IsValidIPV4Address("192.168.1.10:8080")
		So(valid, ShouldBeTrue)
		valid = IsValidIPV4Address("192.168.1.10")
		So(valid, ShouldBeTrue)
		valid = IsValidIPV4Address("192.168.1.abc:8080")
		So(valid, ShouldBeFalse)
		valid = IsValidIPV4Address("192.168.1.10:80:8080")
		So(valid, ShouldBeFalse)
		valid = IsValidIPV4Address("192.168.1.10:abc")
		So(valid, ShouldBeFalse)
	})
}
