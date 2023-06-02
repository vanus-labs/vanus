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

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/bytes"
)

var errInvalidInteger = errors.New("invalid integer")

func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func ConsumeDigits(r io.ByteReader, w io.ByteWriter) (int, byte, error) {
	return bytes.ConsumeUntil(r, w, func(c byte) bool {
		return !IsDigit(c)
	})
}

func ExpectIntegerExt(c byte, s io.ByteScanner) (int, error) {
	if c == '0' {
		return 0, nil
	}

	var err error
	sign := 1
	if c == '-' {
		sign = -1
		c, err = s.ReadByte()
		if err != nil {
			return 0, errInvalidInteger
		}
	}

	if c < '1' || c > '9' {
		return 0, errInvalidInteger
	}
	num := int(c - '0')

	for {
		c, err = s.ReadByte()
		if err != nil {
			if err == io.EOF { //nolint:errorlint // io.EOF is not an error.
				return sign * num, nil
			}
			return 0, err
		}

		if !IsDigit(c) {
			return sign * num, s.UnreadByte()
		}

		// TODO(james.yin): check overflow

		num = num*10 + int(c-'0') //nolint:gomnd // 10 is base
	}
}
