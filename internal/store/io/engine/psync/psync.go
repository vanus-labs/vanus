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

package psync

import (
	// standard libraries.
	"os"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/container/conque/blocking"
	"github.com/vanus-labs/vanus/internal/store/io"
	"github.com/vanus-labs/vanus/internal/store/io/engine"
	"github.com/vanus-labs/vanus/internal/store/io/zone"
)

type writeTask struct {
	f   *os.File
	b   []byte
	off int64
	cb  io.WriteCallback
}

type psync struct {
	q blocking.Queue[writeTask]
}

// Make sure engine implements engine.Interface.
var _ engine.Interface = (*psync)(nil)

func New(opts ...Option) engine.Interface {
	cfg := makeConfig(opts...)
	return new(psync).init(cfg)
}

func (e *psync) init(cfg config) *psync {
	e.q.Init(false)
	for i := 0; i < cfg.parallel; i++ {
		go e.run()
	}
	return e
}

func (e *psync) Close() {
	e.q.Close()
}

func (e *psync) WriteAt(z zone.Interface, b []byte, off int64, so, eo int, cb io.WriteCallback) { //nolint:revive // ok
	// if eo != 0 && eo != len(b) {
	// 	b = b[:eo]
	// }
	// if so != 0 {
	// 	b = b[so:]
	// 	off += int64(so)
	// }
	f, off := z.Raw(off)
	e.q.Push(writeTask{f, b, off, cb})
}

func (e *psync) run() {
	for {
		task, ok := e.q.SharedPop()
		if !ok {
			return
		}

		task.invoke()
	}
}

func (t *writeTask) invoke() {
	// NOTE: data race is ok here.
	t.cb(t.f.WriteAt(t.b, t.off))
}
