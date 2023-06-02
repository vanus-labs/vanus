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

var DummyWriter = &nopWriter{}

type MarkScanner struct {
	Buf []byte
	off int
}

// Make sure MarkScanner implements io.ByteScanner.
var _ io.ByteScanner = (*MarkScanner)(nil)

func NewMarkScanner(b []byte) *MarkScanner {
	return &MarkScanner{Buf: b}
}

func (s *MarkScanner) empty() bool {
	return len(s.Buf) <= s.off
}

func (s *MarkScanner) ReadByte() (byte, error) {
	if s.empty() {
		return 0, io.EOF
	}
	b := s.Buf[s.off]
	s.off++
	return b, nil
}

func (s *MarkScanner) UnreadByte() error {
	if s.off <= 0 {
		return errors.New("reader.UnreadByte: at beginning of slice")
	}
	s.off--
	return nil
}

func (s *MarkScanner) Mark(off int) int {
	return s.off + off
}

func (s *MarkScanner) Since(mark int, off int) []byte {
	return s.Buf[mark : s.off+off]
}

func (s *MarkScanner) From(mark int) []byte {
	return s.Buf[mark:]
}

func (s *MarkScanner) Resume(mark int) error {
	s.off = mark
	return nil
}

func ScannedBytes(s *MarkScanner, mark int, eof bool) []byte {
	if eof {
		return s.Since(mark, 0)
	}
	return s.Since(mark, -1)
}

type nopWriter struct{}

// Make sure nopWriter implements io.Write and io.ByteWriter.
var (
	_ io.Writer     = (*nopWriter)(nil)
	_ io.ByteWriter = (*nopWriter)(nil)
)

func (w *nopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func (w *nopWriter) WriteByte(_ byte) error {
	return nil
}

type CopyOnDiffWriter struct {
	Buf []byte
	new []byte
	off int
}

// Make sure CopyOnDiffWriter implements io.ByteWriter.
var _ io.ByteWriter = (*CopyOnDiffWriter)(nil)

func (w *CopyOnDiffWriter) WriteByte(c byte) error {
	if w.new != nil {
		w.new = append(w.new, c)
		return nil
	}
	if w.off < len(w.Buf) && w.Buf[w.off] == c {
		w.off++
		return nil
	}
	w.new = make([]byte, w.off+1)
	copy(w.new, w.Buf[:w.off])
	w.new[w.off] = c
	return nil
}

func (w *CopyOnDiffWriter) Bytes() []byte {
	if w.new != nil {
		return w.new
	}
	return w.Buf[:w.off]
}

type LastByteWriter interface {
	io.Writer

	LastByte() (byte, bool)
	TruncateLastByte()
}
