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

package vjson_test

import (
	"fmt"
	"testing"

	"github.com/linkall-labs/vanus/internal/trigger/transformation/vjson"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParse(t *testing.T) {
	Convey("parse null", t, func() {
		s := `{"keyNull":null}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
	Convey("parse string", t, func() {
		s := `{"keyString":"string","key\\"":"value\\"value"}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
	Convey("parse num", t, func() {
		s := `{"keyNum":1,"keyNum2":3.14}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
	Convey("parse bool", t, func() {
		s := `{"keyNum":true,"keyNum2":false}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
	Convey("parse obj", t, func() {
		s := `{"keyNum":{"keyNum":"test"}}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
	Convey("parse array", t, func() {
		s := `{"keyNum":[{"keyNum":"test"},1,1.3,"str",true,false]}`
		_, err := vjson.Decode([]byte(s))
		So(err, ShouldBeNil)
	})
}
func TestParseExample(t *testing.T) {
	s := `
{
	"keyNull": null,
	"keyString": "valueString",
	"keyInt": 12,
	"keyFloat" 3.14,
	"keyTrue": true,
	"keyFalse": false,
    "KeyObj": {
      	"keyNull": null,
		"keyString": "valueString",
		"keyInt": 12,
		"keyFloat" 3.14,
      	"keyArr":[
			1,
			3.4,
			"string",
			{"key":"value"},
			[1,"second"],
			null,
			"end"
		],
		"KeyObj": {
			"keyNull": null,
			"keyString": "valueString",
			"keyInt": 12,
			"keyFloat" 3.14,
			"keyArr":[
				1,
				3.4,
				"string",
				{"key":"value"},
				[1,"second"],
				null,
				"end"
			]
    	}
    },
	"keyArr":[
		1,
		3.4,
		"string",
		{"key":"value"},
		[1,"second"],
		null,
		"end"
	]
} 
`
	m, err := vjson.Decode([]byte(s))
	fmt.Println(err)
	for k, v := range m {
		fmt.Println(fmt.Sprintf("%s:%s", k, v.String()))
		if v.Result != nil {
			for k2, v2 := range v.Result {
				fmt.Println(fmt.Sprintf("%s:%s", k2, v2.String()))
			}
		}
	}
}
