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
	// standard libraries.
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
	"github.com/linkall-labs/vanus/internal/store/wal/record"
)

const (
	blockSize = 4 * 1024
)

func TestBlock(t *testing.T) {
	rand.Seed(time.Now().Unix())

	Convey("wal block", t, func() {
		b := Block{
			block: block{
				buf: make([]byte, blockSize),
			},
			SO: 4096,
		}

		Convey("properties", func() {
			So(b.Capacity(), ShouldEqual, blockSize)
			So(b.Size(), ShouldEqual, 0)
			So(b.Committed(), ShouldEqual, 0)
			So(b.Remaining(), ShouldEqual, blockSize)
			So(b.Full(), ShouldBeFalse)
			So(b.WriteOffset(), ShouldEqual, 4096)
		})

		Convey("append", func() {
			dataSize := blockSize - record.HeaderSize*2 + 1
			records := record.Pack(make([]byte, dataSize), dataSize+record.HeaderSize, 0)
			r := records[0]

			n, err := b.Append(r)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, r.Size())
			So(b.Full(), ShouldBeTrue)
			So(b.FullWithOff(n-1), ShouldBeFalse)
			So(b.Committed(), ShouldEqual, 0)
			So(b.WriteOffset(), ShouldEqual, 4096+b.Size())

			Convey("flush", func() {
				b.Flush(io.WriteAtFunc(func(b []byte, base int64, so, eo int, cb io.WriteCallback) {
					So(b[:n], ShouldResemble, r.Marshal())
					So(base, ShouldEqual, 0)

					cb(len(b), nil)
				}), n, 0, func(commit int64, err error) {
					So(err, ShouldBeNil)
					So(commit, ShouldEqual, n)
				})

				So(b.Committed(), ShouldEqual, n)

				b.Flush(nil, 0, 0, func(commit int64, err error) {
					So(err, ShouldBeNil)
					So(commit, ShouldEqual, n)
				})
			})

			Convey("partial flush", func() {
				type writeTask struct {
					so, eo int
					cb     io.WriteCallback
				}

				var flushCount int64
				ch := make(chan writeTask, 16)
				doWrite := func() {
					for t := range ch {
						time.Sleep(time.Duration(rand.Int63n(10)*100) * time.Microsecond)
						t.cb(t.eo-t.so, nil)
						atomic.AddInt64(&flushCount, 1)
					}
				}
				go doWrite()
				go doWrite()

				writer := io.WriteAtFunc(func(b []byte, base int64, so, eo int, cb io.WriteCallback) {
					ch <- writeTask{
						so: so,
						eo: eo,
						cb: cb,
					}
				})

				resultC := make(chan int64, 100)
				wg := sync.WaitGroup{}
				wg.Add(100)
				for i := 1; i <= 100; i++ {
					time.Sleep(time.Duration(rand.Int63n(100)*10) * time.Microsecond)
					b.Flush(writer, i, 0, func(off int64, err error) {
						if err == nil {
							resultC <- off
						}
						wg.Done()
					})
				}
				wg.Wait()
				close(ch)

				So(resultC, ShouldHaveLength, 100)
				for i := 1; i <= 100; i++ {
					So(<-resultC, ShouldEqual, i)
				}
				So(atomic.LoadInt64(&flushCount), ShouldBeLessThanOrEqualTo, 100)
			})
		})

		Convey("append with too large data", func() {
			dataSize := blockSize - record.HeaderSize + 1
			records := record.Pack(make([]byte, dataSize), dataSize+record.HeaderSize, 0)
			r := records[0]

			_, err := b.Append(r)

			So(err, ShouldNotBeNil)
		})
	})
}
