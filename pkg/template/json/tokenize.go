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
	"errors"
	"io"

	// this project.
	"github.com/vanus-labs/vanus/lib/bytes"
	"github.com/vanus-labs/vanus/lib/json/parse"
)

var (
	errInvalidDynamicString = errors.New("invalid dynamic string")
	errInvalidString        = errors.New("invalid JSON string")
	errInvalidNumber        = errors.New("invalid JSON number")
	errInvalidNull          = errors.New("invalid JSON null")
	errInvalidTrue          = errors.New("invalid JSON true")
	errInvalidFalse         = errors.New("invalid JSON false")
)

const (
	// The lowest and highest control characters.
	locc = 0x00
	hicc = 0x1F
)

const dynamicStringPlan = "" +
	`................................` + // 0x00
	`..s............s............s.s.` + // 0x20, double quote(0x22), slash(0x2f), angled brackets(0x3c,0x3e)
	`............................s...` + // 0x40, backslash(0x5c)
	"..\b...\f.......\n...\r.\tu.........." + // 0x60
	`................................` + // 0x80
	`................................` + // 0xa0
	`................................` + // 0xc0
	`................................` //  0xe0

func skipWhitespace(r io.ByteReader) (byte, error) {
	return bytes.IgnoreCount(parse.SkipWhitespace(r))
}

func consumeDynamicString(r io.ByteReader, w io.ByteWriter) (bool, error) {
	for {
		c, err := r.ReadByte()
		if err != nil {
			return false, errInvalidDynamicString
		}

		switch c {
		case '"': // quotation mark, end of string
			return false, nil
		case '<': // open angled bracket, begin of variable
			return true, nil
		case '\\': // reverse solidus
			err = bytes.ConsumeEscaped(r, w, dynamicStringPlan)
			if err != nil {
				return false, errInvalidDynamicString
			}
		default:
			if c <= hicc { // control characters
				return false, errInvalidDynamicString
			}
			if err = w.WriteByte(c); err != nil {
				return false, errInvalidDynamicString
			}
		}
	}
}

func unescapeBracket(bs []byte) []byte {
	buf := bytes.CopyOnDiffWriter{Buf: bs}
	for i := 0; i < len(bs); i++ {
		if bs[i] != '\\' {
			_ = buf.WriteByte(bs[i])
			continue
		}

		i++
		switch bs[i] {
		case '<', '>':
			_ = buf.WriteByte(bs[i])
		case 'u':
			_ = buf.WriteByte('\\')
			_ = buf.WriteByte('u')
			_ = buf.WriteByte(bs[i+1])
			_ = buf.WriteByte(bs[i+2])
			_ = buf.WriteByte(bs[i+3])
			_ = buf.WriteByte(bs[i+4])
			i += 4
		default:
			_ = buf.WriteByte('\\')
			_ = buf.WriteByte(bs[i])
		}
	}
	return buf.Bytes()
}

type number struct {
	negative, negExp            bool
	integer, fraction, exponent []byte
}

func expectNumberExt(c byte, s *bytes.MarkScanner) (number, error) {
	var num number
	var err error
	var n int

	if c == '-' {
		num.negative = true
		// at least one digit
		if c, err = s.ReadByte(); err != nil {
			return num, errInvalidNumber
		}
	}

	if !parse.IsDigit(c) {
		return num, errInvalidNumber
	}

	m := s.Mark(-1)
	_, eof, c, err := consumeDigits(s, bytes.DummyWriter)
	if err != nil {
		return num, errInvalidNumber
	}
	num.integer = bytes.ScannedBytes(s, m, eof)

	// fraction
	if c == '.' {
		m = s.Mark(0)
		n, eof, c, err = consumeDigits(s, bytes.DummyWriter)
		if err != nil || n == 0 {
			return num, errInvalidNumber
		}
		num.fraction = bytes.ScannedBytes(s, m, eof)
	}

	// exponent
	if c == 'E' || c == 'e' {
		m = s.Mark(0)
		n, eof, c, err = consumeDigits(s, bytes.DummyWriter)
		if err != nil {
			return num, errInvalidNumber
		}
		if n == 0 {
			switch c {
			case '+':
			case '-':
				num.negExp = true
			default:
				return num, errInvalidNumber
			}
			m = s.Mark(0)
			n, eof, c, err = consumeDigits(s, bytes.DummyWriter)
			if err != nil || n == 0 {
				return num, errInvalidNumber
			}
		}
		num.exponent = bytes.ScannedBytes(s, m, eof)
	}

	if c != 0 {
		err = s.UnreadByte()
	}
	return num, err
}

func consumeDigits(r io.ByteReader, w io.ByteWriter) (int, bool, byte, error) {
	return bytes.AcceptEOF(parse.ConsumeDigits(r, w))
}

func exceptNullExt(r io.ByteReader) error {
	c, err := r.ReadByte()
	if err != nil || c != 'u' {
		return errInvalidNull
	}
	c, err = r.ReadByte()
	if err != nil || c != 'l' {
		return errInvalidNull
	}
	c, err = r.ReadByte()
	if err != nil || c != 'l' {
		return errInvalidNull
	}
	return nil
}

func exceptTrueExt(r io.ByteReader) error {
	c, err := r.ReadByte()
	if err != nil || c != 'r' {
		return errInvalidTrue
	}
	c, err = r.ReadByte()
	if err != nil || c != 'u' {
		return errInvalidTrue
	}
	c, err = r.ReadByte()
	if err != nil || c != 'e' {
		return errInvalidTrue
	}
	return nil
}

func exceptFalseExt(r io.ByteReader) error {
	c, err := r.ReadByte()
	if err != nil || c != 'a' {
		return errInvalidFalse
	}
	c, err = r.ReadByte()
	if err != nil || c != 'l' {
		return errInvalidFalse
	}
	c, err = r.ReadByte()
	if err != nil || c != 's' {
		return errInvalidFalse
	}
	c, err = r.ReadByte()
	if err != nil || c != 'e' {
		return errInvalidFalse
	}
	return nil
}
