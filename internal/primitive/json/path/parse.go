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
	"errors"
	"strings"
	"unicode/utf8"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
	"github.com/vanus-labs/vanus/internal/primitive/json/parse"
)

var errInvalidJSONPath = errors.New("invalid JSON path")

func Parse(text string) (Path, error) {
	s := bytes.NewMarkScanner(bytes.UnsafeFromString(text))
	if bytes.ExpectChar(s, '$') != nil { // root identifier
		return nil, errInvalidJSONPath
	}
	segments, err := ConsumeSegments(s)
	if err != nil {
		return nil, errInvalidJSONPath
	}
	return &rootPath{segments: segments}, nil
}

func ConsumeExt(c byte, s *bytes.MarkScanner) (Path, error) {
	if c != '$' {
		return nil, errInvalidJSONPath
	}
	segments, err := ConsumeSegments(s)
	if err != nil {
		return nil, errInvalidJSONPath
	}
	return &rootPath{segments: segments}, nil
}

func ConsumeSegments(s *bytes.MarkScanner) ([]Segment, error) {
	var segments []Segment
	for {
		m := s.Mark(0)
		segment, err := consumeSegment(s)
		if err != nil {
			_ = s.Resume(m)
			if len(segments) == 0 {
				return nil, err
			}
			return segments, nil
		}
		segments = append(segments, segment)
	}
}

func consumeSegment(s *bytes.MarkScanner) (Segment, error) {
	c, err := s.ReadByte()
	if err != nil {
		return nil, err
	}

	switch c {
	case '[':
		return consumeBracketedSelection(s)
	case '.':
		return consumeDotSegment(s)
	default:
		return nil, errInvalidJSONPath
	}
}

func consumeBracketedSelection(s *bytes.MarkScanner) (Segment, error) {
	var selectors []Selector
	for {
		_, c, err := parse.SkipWhitespace(s)
		if err != nil {
			return nil, err
		}

		selector, err := consumeSelectorExt(c, s)
		if err != nil {
			return nil, err
		}

		selectors = append(selectors, selector)

		_, c, err = parse.SkipWhitespace(s)
		if err != nil {
			return nil, err
		}

		switch c {
		case ']': // end of bracketed selection
			return &bracketedSelection{selectors: selectors}, nil
		case ',': // continue
		default:
			return nil, errInvalidJSONPath
		}
	}
}

func consumeSelectorExt(c byte, s *bytes.MarkScanner) (Selector, error) {
	switch c {
	case '\'': // name selector
		var b strings.Builder
		if err := parse.ConsumeSingleQuotedString(s, &b); err != nil {
			return nil, err
		}
		return &nameSelector{member: b.String()}, nil
	case '"': // name selector
		var b strings.Builder
		if err := parse.ConsumeDoubleQuotedString(s, &b); err != nil {
			return nil, err
		}
		return &nameSelector{member: b.String()}, nil
	case '*': // wildcard selector
		return &wildcardSelector{}, nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return consumeIndexOrSliceSelector(c, s)
	case ':': // array slice selector
		return consumeSliceSelector(nil, s)
	case '?': // filter selector
		// TODO(james.yin)
		return nil, errors.New("not implemented filter selector")
	}
	return nil, errInvalidJSONPath
}

func consumeIndexOrSliceSelector(c byte, s *bytes.MarkScanner) (Selector, error) {
	indexOrStart, err := parse.ExpectIntegerExt(c, s)
	if err != nil {
		return nil, err
	}

	m := s.Mark(0)
	if _, c, err = parse.SkipWhitespace(s); err != nil {
		return nil, err
	}

	// array slice selector
	if c == ':' {
		return consumeSliceSelector(&indexOrStart, s)
	}

	_ = s.Resume(m)
	return &indexSelector{index: indexOrStart}, nil
}

func consumeSliceSelector(start *int, s *bytes.MarkScanner) (*arraySliceSelector, error) {
	m := s.Mark(0)
	_, c, err := parse.SkipWhitespace(s)
	if err != nil {
		_ = s.Resume(m)
		return nil, err
	}

	var end *int

	me := s.Mark(0)
	e, err := parse.ExpectIntegerExt(c, s)
	if err != nil {
		_ = s.Resume(me)
	} else {
		end = &e
		m = s.Mark(0)
		if _, c, err = parse.SkipWhitespace(s); err != nil {
			return nil, err
		}
	}

	step := 1
	if c == ':' {
		m = s.Mark(0)
		if _, c, err = parse.SkipWhitespace(s); err != nil {
			return nil, err
		}
		if step, err = parse.ExpectIntegerExt(c, s); err != nil {
			step = 1
			_ = s.Resume(m)
		}
	} else {
		_ = s.Resume(m)
	}

	return &arraySliceSelector{
		start: start,
		end:   end,
		step:  step,
	}, nil
}

func consumeDotSegment(s *bytes.MarkScanner) (Segment, error) {
	r, _ := bytes.ReadRune(s)
	switch r {
	case '*': // wildcard selector
		return &wildcardSelector{}, bytes.ExpectChar(s, ']')
	case '.': // descendant segment
		// TODO(james.yin)
		return nil, errors.New("not implemented descendant segment")
	default: // member-name-shorthand
		return consumeMemberNameShorthandExt(r, s)
	}
}

func consumeMemberNameShorthandExt(r rune, s *bytes.MarkScanner) (Segment, error) {
	var b strings.Builder

	if r == utf8.RuneError || !parse.ExceptNameFirst(r) {
		return nil, errInvalidJSONPath
	}
	if _, err := b.WriteRune(r); err != nil {
		return nil, err
	}

	for {
		m := s.Mark(0)
		r, _ = bytes.ReadRune(s)
		if r == utf8.RuneError || !parse.ExceptNameChar(r) {
			_ = s.Resume(m)
			return &nameSelector{member: b.String()}, nil
		}
		if _, err := b.WriteRune(r); err != nil {
			return nil, err
		}
	}
}
