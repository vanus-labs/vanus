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

package bytes

import (
	// standard libraries.
	"bytes"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

const escapePlan = "" +
	`................................` + // 0x00
	`................oooooooo....s.s.` + // 0x20, 0-7(0x30-37), angled brackets(0x3c,0x3e)
	`............................s...` + // 0x40, backslash(0x5c)
	"..\b...\f.......\n...\r.\tu..x......." + // 0x60
	`................................` + // 0x80
	`................................` + // 0xa0
	`................................` + // 0xc0
	`................................` //  0xe0

func TestEscape(t *testing.T) {
	Convey("unicode character", t, func() {
		Convey("basic multilingual plane", func() {
			r := bytes.NewReader([]byte(`20AC`))
			ru, err := ExpectUnicodeChar(r)
			So(err, ShouldBeNil)
			So(ru, ShouldEqual, '\u20AC') // '€'
		})

		Convey("supplementary plane", func() {
			r := bytes.NewReader([]byte(`D801\uDC37`))
			ru, err := ExpectUnicodeChar(r)
			So(err, ShouldBeNil)
			So(ru, ShouldEqual, '\U00010437') // '𐐷'
		})
	})

	Convey("hexadecimal character", t, func() {
		r := bytes.NewReader([]byte(`12`))
		c, err := ExpectHexChar(r)
		So(err, ShouldBeNil)
		So(c, ShouldEqual, '\x12')
	})

	Convey("octal character", t, func() {
		r := bytes.NewReader([]byte(`23`))
		c, err := ExpectOctCharExt('1', r)
		So(err, ShouldBeNil)
		So(c, ShouldEqual, '\123')
	})

	Convey("escaped character", t, func() {
		Convey("invalid", func() {
			r := bytes.NewReader([]byte{0})
			buf := bytes.NewBuffer(nil)
			err := ConsumeEscaped(r, buf, escapePlan)
			So(err, ShouldNotBeNil)
		})

		Convey("self", func() {
			r := bytes.NewReader([]byte{'\\'})
			buf := bytes.NewBuffer(nil)
			err := ConsumeEscaped(r, buf, escapePlan)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "\\")
		})

		Convey("unicode character", func() {
			Convey("basic multilingual plane", func() {
				r := bytes.NewReader([]byte(`u20AC`))
				buf := bytes.NewBuffer(nil)
				err := ConsumeEscaped(r, buf, escapePlan)
				So(err, ShouldBeNil)
				So(buf.String(), ShouldEqual, "\u20AC") // '€'
			})

			Convey("supplementary plane", func() {
				r := bytes.NewReader([]byte(`uD801\uDC37`))
				buf := bytes.NewBuffer(nil)
				err := ConsumeEscaped(r, buf, escapePlan)
				So(err, ShouldBeNil)
				So(buf.String(), ShouldEqual, "\U00010437") // '𐐷'
			})
		})

		Convey("hexadecimal character", func() {
			r := bytes.NewReader([]byte(`x12`))
			buf := bytes.NewBuffer(nil)
			err := ConsumeEscaped(r, buf, escapePlan)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "\x12")
		})

		Convey("octal character", func() {
			r := bytes.NewReader([]byte(`123`))
			buf := bytes.NewBuffer(nil)
			err := ConsumeEscaped(r, buf, escapePlan)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "\123")
		})

		Convey("escaped", func() {
			r := bytes.NewReader([]byte(`n`))
			buf := bytes.NewBuffer(nil)
			err := ConsumeEscaped(r, buf, escapePlan)
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "\n")
		})
	})
}
