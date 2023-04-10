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
	"errors"
	"io"
)

var errUnexpectedChar = errors.New("unexpected character")

func ExpectChar(r io.ByteReader, c byte) error {
	b, err := r.ReadByte()
	if err != nil || b != c {
		return errUnexpectedChar
	}
	return nil
}

func ConsumeUntil(r io.ByteReader, w io.ByteWriter, stop func(byte) bool) (int, byte, error) {
	for count := 0; ; count++ {
		c, err := r.ReadByte()
		if err != nil {
			return count, 0, err
		}
		if stop(c) {
			return count, c, nil
		}
		if err = w.WriteByte(c); err != nil {
			return count, c, err
		}
	}
}

func Skip(r io.ByteReader, expect func(byte) bool) (int, byte, error) {
	for count := 0; ; count++ {
		c, err := r.ReadByte()
		if err != nil {
			return count, 0, err
		}
		if !expect(c) {
			return count, c, nil
		}
	}
}

func IgnoreCount(_ int, c byte, err error) (byte, error) {
	return c, err
}

func AcceptEOF(count int, c byte, err error) (int, bool, byte, error) {
	switch {
	case err == nil:
		return count, false, c, nil
	case err == io.EOF: //nolint:errorlint // io.EOF is not an error
		return count, true, 0, nil
	default:
		return 0, false, 0, err
	}
}

func Unread(r io.ByteScanner, eof bool, err error) error {
	if err == nil && !eof {
		err = r.UnreadByte()
	}
	return err
}
