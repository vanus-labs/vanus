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

package block

import (
	// this project.
	"github.com/vanus-labs/vanus/server/store/io"
)

type oneshot struct {
	base int64
	buf  []byte
}

// Make sure block implements Interface.
var _ Interface = (*oneshot)(nil)

func Oneshot(base int64, data []byte) Interface {
	return &oneshot{
		base: base,
		buf:  data,
	}
}

func (o *oneshot) Base() int64 {
	return o.base
}

func (o *oneshot) Capacity() int {
	return len(o.buf)
}

func (o *oneshot) Flush(writer io.WriterAt, cb FlushCallback) {
	writer.WriteAt(o.buf, o.base, 0, o.Capacity(), cb)
}
