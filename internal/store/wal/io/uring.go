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

package io

import (
	// standard libraries.
	"context"
	"os"
	"sort"

	// third-party libraries.
	"github.com/iceber/iouring-go"

	// this project.
	"github.com/linkall-labs/vanus/observability/log"
)

const (
	defaultResultBufferSize   = 64
	defaultInflightBufferSize = defaultResultBufferSize
)

type uRing struct {
	uring     *iouring.IOURing
	resultc   chan iouring.Result
	inflightc chan uint64
	seq       uint64
}

// Make sure uRing implements Engine.
var _ Engine = (*uRing)(nil)

func NewURing() Engine {
	uring, err := iouring.New(defaultResultBufferSize)
	if err != nil {
		log.Error(context.Background(), "Create iouring failed.", map[string]interface{}{
			log.KeyError: err,
		})
		panic(err)
	}

	e := &uRing{
		uring:     uring,
		resultc:   make(chan iouring.Result, defaultResultBufferSize),
		inflightc: make(chan uint64, defaultInflightBufferSize),
	}

	go e.runCallback()

	return e
}

func (e *uRing) Close() {
	if err := e.uring.Close(); err != nil {
		log.Error(context.Background(), "Encounter error when close iouring.", map[string]interface{}{
			log.KeyError: err,
		})
	}
	close(e.resultc)
}

type pendingResult struct {
	seq uint64
	re  iouring.Result
}

func (e *uRing) runCallback() {
	results := make(map[uint64]iouring.Result)
	pendingResults := make([]pendingResult, 0, defaultInflightBufferSize)

	for result := range e.resultc {
		seq, _ := result.GetRequestInfo().(uint64)
		need := <-e.inflightc

		if seq == need {
			if len(pendingResults) == 0 {
				_ = result.Callback()
			} else {
				pendingResults = append(pendingResults, pendingResult{
					seq: need,
					re:  result,
				})
			}
			continue
		}

		// Buffer and reorder before trigger callback.

		re := results[need]
		if re != nil {
			delete(results, need)
		}
		pendingResults = append(pendingResults, pendingResult{
			seq: need,
			re:  re,
		})

		if seq > need {
			results[seq] = result
		} else {
			if seq == pendingResults[0].seq {
				_ = result.Callback()
				pendingResults = pendingResults[1:]
				for len(pendingResults) != 0 {
					if pendingResults[0].re == nil {
						break
					}
					_ = pendingResults[0].re.Callback()
					pendingResults = pendingResults[1:]
				}
			} else {
				i := sort.Search(len(pendingResults)-1, func(i int) bool {
					return pendingResults[i].seq >= seq
				})
				pendingResults[i].re = result
			}
		}
	}
}

func (e *uRing) WriteAt(f *os.File, b []byte, off int64, cb WriteCallback) {
	seq := e.generateSeq()

	preq := iouring.Pwrite(int(f.Fd()), b, uint64(off)).
		WithInfo(seq).
		WithCallback(func(result iouring.Result) error {
			cb(result.ReturnInt())
			return nil
		})

	_, err := e.uring.SubmitRequest(preq, e.resultc)
	if err != nil {
		cb(0, err)
		return
	}

	e.inflightc <- seq
}

func (e *uRing) generateSeq() uint64 {
	seq := e.seq
	e.seq++
	return seq
}
