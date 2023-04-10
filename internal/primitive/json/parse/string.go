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

package parse

import (
	// standard libraries.
	"errors"
	"io"
)

var errInvalidString = errors.New("invalid string")

const hicc = 0x1F // highest control characters.

func ConsumeDoubleQuotedString(r io.ByteReader, w io.ByteWriter) error {
	for {
		c, err := r.ReadByte()
		if err != nil {
			return errInvalidString
		}

		switch c {
		case '"': // double quotes, end of string
			return nil
		case '\\': // backslash
			if err = ConsumeEscapedWithDoubleQuote(r, w); err != nil {
				return errInvalidString
			}
		default:
			if c <= hicc { // control characters
				return errInvalidString
			}
			if err = w.WriteByte(c); err != nil {
				return errInvalidString
			}
		}
	}
}

func ConsumeSingleQuotedString(r io.ByteReader, w io.ByteWriter) error {
	for {
		c, err := r.ReadByte()
		if err != nil {
			return errInvalidString
		}

		switch c {
		case '\'': // single quote, end of string
			return nil
		case '\\': // backslash
			if err = ConsumeEscapedWithSingleQuote(r, w); err != nil {
				return errInvalidString
			}
		default:
			if c <= hicc { // control characters
				return errInvalidString
			}
			if err = w.WriteByte(c); err != nil {
				return errInvalidString
			}
		}
	}
}
