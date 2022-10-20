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

func TestLookupData(t *testing.T) {
	Convey("test lookup data", t, func() {
		data := map[string]interface{}{
			"map": map[string]interface{}{
				"number": 123.4,
				"str":    "str",
			},
			"str":   "stringV",
			"array": []interface{}{1.1, "str"},
		}
		Convey("get string", func() {
			v, err := LookupData(data, "$.str")
			So(err, ShouldBeNil)
			So(v, ShouldEqual, "stringV")
		})
		Convey("get map", func() {
			v, err := LookupData(data, "$.map")
			So(err, ShouldBeNil)
			So(len(v.(map[string]interface{})), ShouldEqual, 2)
		})
		Convey("get array", func() {
			v, err := LookupData(data, "$.array")
			So(err, ShouldBeNil)
			So(len(v.([]interface{})), ShouldEqual, 2)
		})
	})
}
