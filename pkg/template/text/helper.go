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

package text

import (
	// standard libraries.
	"io"
	"sync"

	// third-party libraries.
	"github.com/ohler55/ojg/oj"

	"github.com/vanus-labs/vanus/lib/bytes"
)

var writerPool = sync.Pool{
	New: func() any {
		return &oj.Writer{Options: oj.DefaultOptions}
	},
}

func writeJSON(w io.Writer, v any) error {
	writer, _ := writerPool.Get().(*oj.Writer)
	defer writerPool.Put(writer)
	return oj.Write(w, v, writer)
}

func write(w io.Writer, v any) error {
	switch val := v.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return writeJSON(w, v)
	case string:
		return ignoreCount(w.Write(bytes.UnsafeFromString(val)))
	default:
		return writeJSON(w, v)
	}
}

func ignoreCount(_ int, err error) error {
	return err
}
