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

package main

import (
	// standard libraries.
	"context"
	"fmt"
	"log"
	stdsync "sync"
	"sync/atomic"
	"time"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/sync"
	walog "github.com/vanus-labs/vanus/internal/store/wal"
)

const (
	N           = 500000
	PayloadSize = 1024
)

func main() {
	ctx := context.Background()

	wal, err := walog.Open(
		ctx,
		"wal-test",
		// walog.WithBlockSize(4*1024),
		// walog.WithFileSize(1024*1024*1024),
		walog.WithFlushDelayTime(10*time.Millisecond),
		// walog.WithIOEngine(uring.New()),
		// walog.WithIOEngine(psync.New(psync.WithParallel(8))),
	)
	if err != nil {
		panic(err)
	}

	defer func() {
		wal.Close()
		wal.Wait()
	}()

	payload := generatePayload(PayloadSize)

	var count, last int64
	var totalCost, lastCost int64
	var totalWrite, lastWrite int64

	go func() {
		for {
			time.Sleep(time.Second)
			cu := atomic.LoadInt64(&count)
			ct := atomic.LoadInt64(&totalCost)
			cw := atomic.LoadInt64(&totalWrite)
			if n := cu - last; n != 0 {
				log.Printf("TPS: %d\tlatency: %.3fms\tthroughput: %.2fMiB/s\n",
					n, float64(ct-lastCost)/float64(n)/1000, float64(cw-lastWrite)/1024/1024)
			} else {
				log.Printf("TPS: %d\tlatency: NaN\n", n)
			}
			last = cu
			lastCost = ct
			lastWrite = cw
		}
	}()

	start := time.Now()

	var sema sync.Semaphore
	for i := 0; i < 256; i++ {
		sema.Release()
	}

	var wg stdsync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		sema.Acquire()
		st := time.Now()
		wal.AppendOne(ctx, payload, func(r walog.Range, err error) {
			cost := time.Since(st)
			if err != nil {
				log.Printf("err: %v", err)
			} else {
				atomic.AddInt64(&count, 1)
				atomic.AddInt64(&totalCost, cost.Microseconds())
				atomic.AddInt64(&totalWrite, r.EO-r.SO)
			}
			wg.Done()
			sema.Release()
		})
	}
	wg.Wait()

	cost := time.Since(start)
	fmt.Printf("write: %.2f MiB\n", float64(totalCost)/1024/1024)
	fmt.Printf("cost: %d ms\n", cost.Milliseconds())
	fmt.Printf("failed: %d\n", N-count)
	fmt.Printf("tps: %f\n", float64(count)/cost.Seconds())
}

func generatePayload(size int) []byte {
	data := func() string {
		str := ""
		for idx := 0; idx < size-1; idx++ {
			str += "a"
		}
		str += "\n"
		return str
	}()
	return []byte(data)
}
