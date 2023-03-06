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

//go:build linux
// +build linux

package uring

import (
	// standard libraries.
	"context"

	// third-party libraries.
	"github.com/iceber/iouring-go"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/io"
	"github.com/vanus-labs/vanus/internal/store/io/engine"
	"github.com/vanus-labs/vanus/internal/store/io/zone"
)

const (
	defaultResultBufferSize = 64
)

type uRing struct {
	ring    *iouring.IOURing
	resultC chan iouring.Result
}

// Make sure uRing implements engine.Interface.
var _ engine.Interface = (*uRing)(nil)

func New() engine.Interface {
	ring, err := iouring.New(defaultResultBufferSize)
	if err != nil {
		log.Error(context.Background(), "Create iouring failed.", map[string]interface{}{
			log.KeyError: err,
		})
		panic(err)
	}

	e := &uRing{
		ring:    ring,
		resultC: make(chan iouring.Result, defaultResultBufferSize),
	}

	go e.runCallback()

	return e
}

func (e *uRing) Close() {
	if err := e.ring.Close(); err != nil {
		log.Error(context.Background(), "Encounter error when close iouring.", map[string]interface{}{
			log.KeyError: err,
		})
	}
	close(e.resultC)
}

func (e *uRing) runCallback() {
	for result := range e.resultC {
		_ = result.Callback()
	}
}

func (e *uRing) WriteAt(z zone.Interface, b []byte, off int64, so, eo int, cb io.WriteCallback) {
	f, offset := z.Raw(off)
	pr := iouring.Pwrite(int(f.Fd()), b, uint64(offset)).
		WithCallback(func(result iouring.Result) error {
			cb(result.ReturnInt())
			return nil
		})

	_, err := e.ring.SubmitRequest(pr, e.resultC)
	if err != nil {
		cb(0, err)
		return
	}
}
