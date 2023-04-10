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
	"unicode/utf8"
)

var (
	errInvalidEscapeChar  = errors.New("invalid escape character")
	errInvalidUnicodeChar = errors.New("invalid unicode character")
	errInvalidHexChar     = errors.New("invalid hexadecimal character")
	errInvalidOctChar     = errors.New("invalid octal character")
)

const (
	highSurrogateMin = 0xD800
	highSurrogateMax = 0xDBFF
	lowSurrogateMin  = 0xDC00
	lowSurrogateMax  = 0xDFFF
)

var (
	octBitmap [256]bool
	hexBitmap [256]bool
	hexToByte [256]byte
	hexToRune [256]rune
)

func init() { //nolint:gochecknoinits // init constant table
	for i := '0'; i <= '7'; i++ {
		octBitmap[i] = true
	}

	for i := '0'; i <= '9'; i++ {
		hexBitmap[i] = true
		hexToRune[i] = i - '0'
		hexToByte[i] = byte(hexToRune[i])
	}
	shift := 'a' - 'A'
	for i := 'A'; i <= 'F'; i++ {
		hexBitmap[i] = true
		hexBitmap[i+shift] = true
		hexToRune[i] = i - 'A' + 10 //nolint:gomnd // base number
		hexToRune[i+shift] = hexToRune[i]
		hexToByte[i] = byte(hexToRune[i])
		hexToByte[i+shift] = hexToByte[i]
	}
}

func ConsumeEscaped(r io.ByteReader, w io.ByteWriter, plan string) error {
	c, err := r.ReadByte()
	if err != nil {
		return errInvalidEscapeChar
	}
	return consumeEscapedExt(c, r, w, plan)
}

func consumeEscapedExt(c byte, r io.ByteReader, w io.ByteWriter, plan string) error {
	p := UnsafeAt(plan, int(c))
	switch p {
	case '.':
		return errInvalidEscapeChar
	case 's': // Self
		return w.WriteByte(c)
	case 'u': // \uNNNN
		ru, err := ExpectUnicodeChar(r)
		if err != nil {
			return err
		}
		return WriteRune(w, ru)
	case 'x': // \xNN
		cc, err := ExpectHexChar(r)
		if err != nil {
			return err
		}
		return w.WriteByte(cc)
	case 'o': // \NNN
		cc, err := ExpectOctCharExt(c, r)
		if err != nil {
			return err
		}
		return w.WriteByte(cc)
	default:
		return w.WriteByte(p)
	}
}

func ExpectUnicodeChar(r io.ByteReader) (rune, error) {
	hi, err := expectUnicodeSurrogate(r)
	if err != nil {
		return utf8.RuneError, errInvalidUnicodeChar
	}

	// non-surrogate
	if hi < highSurrogateMin || hi > lowSurrogateMax {
		return hi, nil
	}

	// error of high-surrogate
	if hi > highSurrogateMax {
		return utf8.RuneError, errInvalidUnicodeChar
	}

	if ExpectChar(r, '\\') != nil || ExpectChar(r, 'u') != nil {
		return utf8.RuneError, errInvalidUnicodeChar
	}

	lo, err := expectUnicodeSurrogate(r)
	if err != nil {
		return utf8.RuneError, errInvalidUnicodeChar
	}

	// error of low-surrogate
	if lowSurrogateMin < 0xDC00 || lo > lowSurrogateMax {
		return utf8.RuneError, errInvalidUnicodeChar
	}

	return 0x10000 + (hi-highSurrogateMin)<<10 + (lo - lowSurrogateMin), nil
}

func expectUnicodeSurrogate(r io.ByteReader) (rune, error) {
	b0, err := r.ReadByte()
	if err != nil || !hexBitmap[b0] {
		return 0, errInvalidUnicodeChar
	}
	b1, err := r.ReadByte()
	if err != nil || !hexBitmap[b1] {
		return 0, errInvalidUnicodeChar
	}
	b2, err := r.ReadByte()
	if err != nil || !hexBitmap[b2] {
		return 0, errInvalidUnicodeChar
	}
	b3, err := r.ReadByte()
	if err != nil || !hexBitmap[b3] {
		return 0, errInvalidUnicodeChar
	}
	ru := hexToRune[b0]*0x1000 + hexToRune[b1]*0x100 + hexToRune[b2]*0x10 + hexToRune[b3]
	return ru, nil
}

func ExpectHexChar(r io.ByteReader) (byte, error) {
	b0, err := r.ReadByte()
	if err != nil || !hexBitmap[b0] {
		return 0, errInvalidHexChar
	}
	b1, err := r.ReadByte()
	if err != nil || !hexBitmap[b1] {
		return 0, errInvalidHexChar
	}
	return hexToByte[b0]*0x10 + hexToByte[b1], nil
}

func ExpectOctCharExt(b0 byte, r io.ByteReader) (byte, error) {
	b1, err := r.ReadByte()
	if err != nil || !octBitmap[b1] {
		return 0, errInvalidOctChar
	}
	b2, err := r.ReadByte()
	if err != nil || !octBitmap[b2] {
		return 0, errInvalidOctChar
	}
	return (b0-'0')*0o100 + (b1-'0')*0o10 + (b2-'0')*0o1, nil
}
