// Copyright 2022 Linkall Inc.
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

package stream

import "io"

type input struct {
	data []byte
	wp   int
}

// Make sure Data implements io.Reader.
var _ io.Reader = (*input)(nil)

func (in *input) remaining() int {
	return len(in.data) - in.wp
}

func (in *input) eof() bool {
	return in.remaining() == 0
}

func (in *input) advance(sz int) []byte {
	// if sz > in.remaining() {
	// 	panic("no enough data")
	// }
	b := in.data[in.wp : in.wp+sz]
	in.wp += sz
	return b
}

func (in *input) Read(b []byte) (int, error) {
	n := copy(b, in.data[in.wp:])
	in.wp += n
	return n, nil
}
