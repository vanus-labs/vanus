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
	"reflect"
	"unsafe"
)

func UnsafeFromString(s string) []byte {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	return unsafe.Slice((*byte)(unsafe.Pointer(hdr.Data)), hdr.Len)
}

func UnsafeToString(b []byte) string {
	// hdr := (*reflect.StringHeader)(unsafe.Pointer(&b))
	// return *(*string)(unsafe.Pointer(hdr))
	return *(*string)(unsafe.Pointer(&b))
}

// UnsafeSlice implements the same functionality as `s[lo:hi]`, but without bounds check.
func UnsafeSlice[T []byte | string](s T, lo int, hi int) []byte {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	p := unsafe.Add(unsafe.Pointer(hdr.Data), lo)
	return unsafe.Slice((*byte)(p), hi-lo)
}

// UnsafeAt implements the same functionality as `s[pos]`, but without bounds check.
func UnsafeAt[T []byte | string](s T, pos int) byte {
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&s))
	p := unsafe.Add(unsafe.Pointer(hdr.Data), pos)
	return *(*byte)(p)
}
