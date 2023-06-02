// Copyright 2023 Linkall Inc.
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

package json

import (
	// standard libraries.
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestParser(t *testing.T) {
	template := `{
  "foo": "str",
  "bar": [
    <var>,
    "<a0> baz <b1> \</br>\r\u000A<$.c[2].d['e-f g\'"<>'].h>",
    "<h>",
    "que",
    true,
    false,
    null,
    {},
    []
  ],
  "quux": <$["i"].j>,
  "num": -0.0123,
  "empty": {
  }
}`

	Convey("JSON template parser", t, func() {
		var p templateParser
		n, err := p.parse(template)
		So(err, ShouldBeNil)
		_ = n
	})
}
