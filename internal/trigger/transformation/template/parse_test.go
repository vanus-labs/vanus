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

package template

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParse(t *testing.T) {
	Convey("parse constants", t, func() {
		p := NewParser()
		p.Parse("constants")
		So(len(p.GetNodes()), ShouldEqual, 1)
		n := p.GetNodes()[0]
		So(n.Type(), ShouldEqual, Constant)
	})
	Convey("parse variable", t, func() {
		p := NewParser()
		p.Parse("${str}")
		So(len(p.GetNodes()), ShouldEqual, 1)
		n := p.GetNodes()[0]
		So(n.Type(), ShouldEqual, Variable)
		So(n.Value(), ShouldEqual, "str")
	})

	Convey("parse text", t, func() {
		p := NewParser()
		p.Parse("begin ${str} end")
		So(len(p.GetNodes()), ShouldEqual, 3)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, Variable)
		So(n.Value(), ShouldEqual, "str")
	})

	Convey("parse json", t, func() {
		p := NewParser()
		p.Parse(`{"key":"${str}","key2":${str2}}`)
		So(len(p.GetNodes()), ShouldEqual, 5)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str")
		n = p.GetNodes()[2]
		So(n.Type(), ShouldEqual, Constant)
		So(n.Value(), ShouldEqual, `","key2":`)
		n = p.GetNodes()[3]
		So(n.Type(), ShouldEqual, Variable)
		So(n.Value(), ShouldEqual, "str2")
	})
	Convey("parse json with special symbol 1", t, func() {
		p := NewParser()
		p.Parse(` {"key": ":${str}"}`)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str")
	})
	Convey("parse json with special symbol 2", t, func() {
		p := NewParser()
		p.Parse(` {"key": "abc:${str}"}`)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str")
	})
	Convey("parse json with special symbol 3", t, func() {
		p := NewParser()
		p.Parse(` {"key": "\":${str}"}`)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str")
	})
	Convey("parse json with special symbol 4", t, func() {
		p := NewParser()
		p.Parse(` {"key": "\":${str} sdf: ${str2}"}`)
		n := p.GetNodes()[1]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str")
		n = p.GetNodes()[3]
		So(n.Type(), ShouldEqual, StringVariable)
		So(n.Value(), ShouldEqual, "str2")
	})
}
