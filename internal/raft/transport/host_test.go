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

package transport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/raft/raftpb"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHost(t *testing.T) {
	Convey("test host", t, func() {
		resolver := NewSimpleResolver()
		nodeID := uint64(3)
		localaddr := "127.0.0.1:12000"
		resolver.Register(nodeID, localaddr)
		h := NewHost(resolver, localaddr)
		ctx := context.Background()

		ch := make(chan *raftpb.Message, 10)
		h.Register(nodeID, &receiver{
			recvch: ch,
		})
		msg := &raftpb.Message{
			To: nodeID,
		}
		msgLen := 5
		msgs := make([]*raftpb.Message, msgLen)
		for i := 0; i < msgLen; i++ {
			msgs[i] = &raftpb.Message{
				To: nodeID,
			}
		}

		Convey("test host Receive method", func() {
			timeoutCtx, cannel := context.WithTimeout(ctx, 3*time.Second)
			defer cannel()
			err := h.Receive(timeoutCtx, msg, localaddr)
			So(err, ShouldBeNil)

			for i := 0; i < 3; i++ {
				select {
				case m := <-ch:
					So(m, ShouldResemble, msg)
					return
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
			So(false, ShouldBeTrue)
		})

		Convey("test host Send callback", func() {
			timeoutCtx, cannel := context.WithTimeout(ctx, 3*time.Second)
			defer cannel()

			h.Send(timeoutCtx, msg, nodeID, localaddr, func(err error) {})
			for i := 0; i < 3; i++ {
				select {
				case m := <-ch:
					So(m, ShouldResemble, msg)
					return
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
			So(false, ShouldBeTrue)
		})

		Convey("test host Send no endpoint input", func() {
			timeoutCtx, cannel := context.WithTimeout(ctx, 3*time.Second)
			defer cannel()

			h.Send(timeoutCtx, msg, nodeID, "", func(err error) {})
			for i := 0; i < 3; i++ {
				select {
				case m := <-ch:
					So(m, ShouldResemble, msg)
					return
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
			So(false, ShouldBeTrue)
		})

		Convey("test host resolveMultiplexer method", func() {
			h := h.(*host)
			testEndpoint := "127.0.0.1:11000"
			_, ok := h.peers.Load(testEndpoint)
			So(ok, ShouldBeFalse)
			p := h.resolveMultiplexer(ctx, nodeID, testEndpoint)
			So(p, ShouldNotBeNil)
			_, ok = h.peers.Load(testEndpoint)
			So(ok, ShouldBeTrue)
		})

		Convey("test raftNode unreachable", func() {
			var count int64
			ch2 := make(chan struct{}, 1)
			h.Send(ctx, msg, nodeID+1, "", func(err error) {
				if err != nil {
					atomic.AddInt64(&count, 1)
				}
				ch2 <- struct{}{}
			})
			<-ch2
			So(count, ShouldNotBeZeroValue)
		})
	})
}
