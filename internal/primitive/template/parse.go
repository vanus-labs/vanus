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

package template

import (
	// standard libraries.
	"errors"
	"io"
	"unicode"
	"unicode/utf8"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
	jp "github.com/vanus-labs/vanus/internal/primitive/json/path"
)

var errVariable = errors.New("invalid variable")

func ExpectVariable(s *bytes.MarkScanner) ([]byte, jp.Path, error) {
	m := s.Mark(0) // exclude '<'

	c, err := s.ReadByte()
	if err != nil {
		return nil, nil, errVariable
	}

	switch c {
	case '$': // JSON path, begin with the root identifier '$'
		path, err := jp.ConsumeExt(c, s)
		if err != nil {
			return nil, nil, err
		}
		if err = bytes.ExpectChar(s, '>'); err != nil {
			return nil, nil, err
		}
		return s.Since(m, -1), path, nil
	default:
		if err := consumeIdentifierExt(c, s, bytes.DummyWriter); err != nil {
			return nil, nil, err
		}
		return s.Since(m, -1), nil, nil
	}
}

func consumeIdentifierExt(c byte, r io.ByteReader, w io.ByteWriter) error {
	ru, _ := bytes.ReadRuneExt(c, r)

	for {
		if !unicode.Is(IdentifierRangeTable, ru) {
			return errVariable
		}

		if err := bytes.WriteRune(w, ru); err != nil {
			return err
		}

		ru, _ = bytes.ReadRune(r)
		if ru == utf8.RuneError {
			return errVariable
		}

		if ru == '>' { // close angled bracket, end of variable
			return nil
		}
	}
}
