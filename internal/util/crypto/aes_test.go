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

package crypto

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAesCrypt(t *testing.T) {
	Convey("test aes crypt", t, func() {
		key := "testKey"
		value := "value"
		d, err := AESEncrypt(value, key)
		So(err, ShouldBeNil)
		d, err = AESDecrypt(d, key)
		So(err, ShouldBeNil)
		So(d, ShouldEqual, value)
	})
}
