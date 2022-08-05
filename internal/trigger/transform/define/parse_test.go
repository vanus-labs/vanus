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

package define_test

import (
	"testing"

	"github.com/linkall-labs/vanus/internal/trigger/transform/define"

	// third-party.
	. "github.com/smartystreets/goconvey/convey"
)

func TestParse(t *testing.T) {
	Convey("value is empty", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{})
		n := p.GetNodes()
		So(len(n), ShouldEqual, 0)
	})
	Convey("value is black", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "  "})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.Constant)
	})
	Convey("value is constant", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "ctx"})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.Constant)
		So(n.Value, ShouldEqual, "ctx")
	})

	Convey("value is context", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "$.ctx"})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.ContextVariable)
		So(n.Value, ShouldEqual, "ctx")
	})

	Convey("value is data", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "$.data"})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.DataVariable)
		So(n.Value, ShouldEqual, "")
	})

	Convey("value is data one", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "$.data.one"})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.DataVariable)
		So(n.Value, ShouldEqual, "one")
	})

	Convey("value is data two", t, func() {
		p := define.NewParse()
		p.Parse(map[string]string{"k": "$.data.one.two"})
		n, exist := p.GetNode("k")
		So(exist, ShouldBeTrue)
		So(n.Type, ShouldEqual, define.DataVariable)
		So(n.Value, ShouldEqual, "one.two")
	})
}
