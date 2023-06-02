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

package path

import (
	// standard libraries.
	"strings"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
)

func TestParse_consumeSliceSelector(t *testing.T) {
	buildScanner := func(sliceSelector string) *bytes.MarkScanner {
		s := bytes.NewMarkScanner([]byte(sliceSelector))
		m := strings.Index(sliceSelector, ":") + 1
		_ = s.Resume(m)
		return s
	}

	soResult := func(selector, expect *arraySliceSelector) {
		if expect.start == nil {
			So(selector.start, ShouldBeNil)
		} else {
			So(selector.start, ShouldNotBeNil)
			So(*selector.start, ShouldEqual, *expect.start)
		}
		if expect.end == nil {
			So(selector.end, ShouldBeNil)
		} else {
			So(selector.end, ShouldNotBeNil)
			So(*selector.end, ShouldEqual, *expect.end)
		}
		So(selector.step, ShouldEqual, expect.step)
	}

	newInt := func(i int) *int {
		return &i
	}

	cases := []struct {
		seg    string
		expect *arraySliceSelector
	}{
		{"[ : ]", &arraySliceSelector{start: nil, end: nil, step: 1}},
		{"[ : : ]", &arraySliceSelector{start: nil, end: nil, step: 1}},
		{"[ : : -1 ]", &arraySliceSelector{start: nil, end: nil, step: -1}},
		{"[ 0 : : ]", &arraySliceSelector{start: newInt(0), end: nil, step: 1}},
		{"[ : 0 : ]", &arraySliceSelector{start: nil, end: newInt(0), step: 1}},
		{"[ 1 : 3 ]", &arraySliceSelector{start: newInt(1), end: newInt(3), step: 1}},
		{"[ 1 : 5 : 2 ]", &arraySliceSelector{start: newInt(1), end: newInt(5), step: 2}},
		{"[ 5 : 1 : -2 ]", &arraySliceSelector{start: newInt(5), end: newInt(1), step: -2}},
	}

	for i := range cases {
		c := &cases[i]

		Convey("parse array slice selector: "+c.seg, t, func() {
			s := buildScanner(c.seg)

			selector, err := consumeSliceSelector(c.expect.start, s)
			So(err, ShouldBeNil)
			So(s.Mark(0), ShouldEqual, len(c.seg)-2)
			soResult(selector, c.expect)
		})
	}
}

func TestParse_ConsumeSegments(t *testing.T) {
	Convey("parse", t, func() {
		jp := "$.foo['bar'].baz[0].qux"
		s := bytes.NewMarkScanner([]byte(jp))
		s.Resume(1)

		segments, err := ConsumeSegments(s)
		So(err, ShouldBeNil)
		So(s.Mark(0), ShouldEqual, len(jp))
		So(segments, ShouldHaveLength, 5)
	})
}
