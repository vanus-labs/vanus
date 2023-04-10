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

package text

import (
	// standard libraries.
	stdbytes "bytes"

	// third-party libraries.
	"github.com/ohler55/ojg/jp"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
	"github.com/vanus-labs/vanus/internal/primitive/template"
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

func parse(text string) ([]templateSegment, error) {
	bs := bytes.UnsafeFromString(text)

	var segments []templateSegment
	var buf stdbytes.Buffer
	var i, start int

	lazyAppend := func() {
		if start >= i {
			return
		}
		s := bytes.UnsafeSlice(bs, start, i)
		buf.Write(s)
	}
	packText := func() {
		lazyAppend()
		if buf.Len() == 0 {
			return
		}
		segments = append(segments, &textSegment{
			text: append([]byte{}, buf.Bytes()...),
		})
		buf.Reset()
	}
	advance := func(n int) {
		i += n
	}
	next := func(n int) {
		advance(n)
		start = i
	}

	n := len(bs)
	for i < n {
		b := bytes.UnsafeAt(bs, i)
		switch b {
		case '\\':
			lazyAppend()
			s := stdbytes.NewReader(bytes.UnsafeSlice(bs, i+1, n))
			err := bytes.ConsumeEscaped(s, &buf, escapePlan)
			if err != nil {
				return nil, err
			}
			next(n - i + 1 - s.Len())
		case '<':
			packText()
			s := bytes.NewMarkScanner(bytes.UnsafeSlice(bs, i+1, n))
			segment, err := expectVariable(s)
			if err != nil {
				return nil, err
			}
			segments = append(segments, segment)
			next(s.Mark(0) + 1)
		default:
			advance(1)
		}
	}

	i = n // maybe unnecessary?
	packText()

	return segments, nil
}

func expectVariable(s *bytes.MarkScanner) (templateSegment, error) {
	original, path, err := template.ExpectVariable(s)
	if err != nil {
		return nil, err
	}
	if path != nil {
		return &jsonPathSegment{path: jp.MustParse(original)}, nil
	}
	return &variableSegment{name: string(original)}, nil
}
