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
	"io"
	"runtime"
	"sync"
	"time"
	"unsafe"

	// third-party libraries.
	"github.com/ohler55/ojg/oj"

	// first-party libraries.
	"github.com/vanus-labs/vanus/lib/json/generate"
)

var writerPool = sync.Pool{
	New: func() any {
		opts := oj.DefaultOptions
		opts.TimeFormat = time.RFC3339
		return &oj.Writer{Options: opts}
	},
}

func writeJSON(w io.Writer, v any) error {
	writer, _ := writerPool.Get().(*oj.Writer)
	defer writerPool.Put(writer)
	return oj.Write(w, v, writer)
}

func writeJSONInJSONString(w io.Writer, v any) error {
	// TODO(james.yin): optimize it.
	writer, _ := writerPool.Get().(*oj.Writer)
	defer writerPool.Put(writer)
	data, err := oj.Marshal(v, writer)
	if err != nil {
		return err
	}
	str, err := oj.Marshal(string(data), writer)
	if err != nil {
		return err
	}
	return ignoreCount(w.Write(str[1 : len(str)-1]))
}

func writeInJSONString(w io.Writer, v any) error {
	switch v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string:
		var ba [32]byte // underlying array of byte buffer in stack.
		buf := unsafe.Slice((*byte)(noescape(unsafe.Pointer(&ba[0]))), len(ba))
		data := appendInJSONString(buf[:0], v)
		err := ignoreCount(w.Write(data))
		runtime.KeepAlive(ba)
		return err
	default:
		return writeJSONInJSONString(w, v)
	}
}

func appendInJSONString(dst []byte, v any) []byte {
	switch val := v.(type) {
	case nil:
		return generate.AppendNull(dst)
	case bool:
		return generate.AppendBool(dst, val)
	case int:
		return generate.AppendInt(dst, val)
	case int8:
		return generate.AppendInt(dst, val)
	case int16:
		return generate.AppendInt(dst, val)
	case int32:
		return generate.AppendInt(dst, val)
	case int64:
		return generate.AppendInt(dst, val)
	case uint:
		return generate.AppendUint(dst, val)
	case uint8:
		return generate.AppendUint(dst, val)
	case uint16:
		return generate.AppendUint(dst, val)
	case uint32:
		return generate.AppendUint(dst, val)
	case uint64:
		return generate.AppendUint(dst, val)
	case float32:
		return generate.AppendFloat32(dst, val)
	case float64:
		return generate.AppendFloat64(dst, val)
	case string:
		return generate.AppendRawString(dst, val)
	}
	// TODO
	return nil
}

func ignoreCount(_ int, err error) error {
	return err
}

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0) //nolint:staticcheck // copy from go source code.
}
