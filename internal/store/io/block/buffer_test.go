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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
)

const (
	bufferSize = 4 * 1024
)

func TestBuffer(t *testing.T) {
	rand.Seed(time.Now().Unix())

	Convey("buffer", t, func() {
		b := Buffer{
			base: 4096,
			buf:  make([]byte, bufferSize),
		}

		Convey("properties", func() {
			So(b.Base(), ShouldEqual, 4096)
			So(b.Capacity(), ShouldEqual, bufferSize)
			So(b.Size(), ShouldEqual, 0)
			So(b.Committed(), ShouldEqual, 0)
			So(b.Remaining(), ShouldEqual, bufferSize)
			So(b.Full(), ShouldBeFalse)
		})

		Convey("append", func() {
			dataSize := bufferSize - 1
			data := make([]byte, bufferSize)
			rand.Read(data)
			r := bytes.NewBuffer(data[:dataSize])

			n, err := b.Append(r)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, dataSize)
			So(b.Size(), ShouldEqual, dataSize)
			So(b.Committed(), ShouldEqual, 0)
			So(b.Remaining(), ShouldEqual, 1)
			So(b.Full(), ShouldBeFalse)

			Convey("flush", func() {
				b.Flush(io.WriteAtFunc(func(b []byte, base int64, so, eo int, cb io.WriteCallback) {
					So(b[:dataSize], ShouldResemble, data[:dataSize])
					So(b[dataSize:], ShouldResemble, []byte{0})
					So(base, ShouldEqual, 4096)
					So(so, ShouldEqual, 0)
					So(eo, ShouldEqual, dataSize)

					cb(len(b), nil)
				}), func(off int, err error) {
					So(err, ShouldBeNil)
					So(off, ShouldEqual, dataSize)
				})

				So(b.Committed(), ShouldEqual, dataSize)

				b.Flush(nil, func(off int, err error) {
					So(err, ShouldBeError, ErrAlreadyFlushed)
					So(off, ShouldEqual, n)
				})

				n, err := b.Append(bytes.NewBuffer(data[dataSize:]))
				So(err, ShouldBeNil)
				So(n, ShouldEqual, bufferSize)
				So(b.Size(), ShouldEqual, bufferSize)
				So(b.Committed(), ShouldEqual, dataSize)
				So(b.Remaining(), ShouldEqual, 0)
				So(b.Full(), ShouldBeTrue)

				b.Flush(io.WriteAtFunc(func(b []byte, base int64, so, eo int, cb io.WriteCallback) {
					So(b, ShouldResemble, data)
					So(base, ShouldEqual, 4096)
					So(so, ShouldEqual, dataSize)
					So(eo, ShouldEqual, bufferSize)

					cb(len(b), nil)
				}), func(off int, err error) {
					So(err, ShouldBeNil)
					So(off, ShouldEqual, bufferSize)
				})
			})
		})

		Convey("sequence flush", func() {
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

			resultC := make(chan int, 100)
			wg := sync.WaitGroup{}
			wg.Add(100)
			for i := 1; i <= 100; i++ {
				time.Sleep(time.Duration(rand.Int63n(100)*10) * time.Microsecond)
				b.Append(bytes.NewReader([]byte{byte(rand.Intn(256))}))
				b.Flush(writer, func(off int, err error) {
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

		Convey("append with too large data", func() {
			dataSize := bufferSize + 1
			data := make([]byte, dataSize)
			rand.Read(data)
			r := bytes.NewBuffer(data)

			n, err := b.Append(r)

			So(err, ShouldBeNil)
			So(n, ShouldEqual, bufferSize)
		})
	})
}
