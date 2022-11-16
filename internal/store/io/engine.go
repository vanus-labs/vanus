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

package io

import "os"

type Engine interface {
	Close()
	// WriteAt writes block b to the File starting at byte offset off.
	// If only partial data is changed, offset so and eo are used to hint it.
	// WriteCallback cb is called with the number of bytes written and an error when the operation completes.
	WriteAt(f *os.File, b []byte, off int64, so, eo int, cb WriteCallback)
}

type engine struct{}

// Make sure engine implements Engine.
var _ Engine = (*engine)(nil)

func NewEngine() Engine {
	return &engine{}
}

func (e *engine) Close() {}

func (e *engine) WriteAt(f *os.File, b []byte, off int64, so, eo int, cb WriteCallback) {
	cb(len(b), nil)
	//cb(f.WriteAt(b, off))
}
