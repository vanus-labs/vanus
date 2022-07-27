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
	"testing"
	"time"

	"github.com/linkall-labs/vanus/raft/raftpb"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHost(t *testing.T) {
	resolver := NewSimpleResolver()
	localaddr := "127.0.0.1:12000"
	h := NewHost(resolver, localaddr)
	ctx := context.Background()
	nodeID := uint64(3)
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

	Convey("test host Receive method", t, func() {
		err := h.Receive(ctx, msg, localaddr)
		So(err, ShouldBeNil)

		var m *raftpb.Message
		timer := time.NewTimer(3 * time.Second)

	loop:
		for {
			select {
			case m = <-ch:
				So(m, ShouldResemble, msg)
				break loop
			case <-timer.C:
				So(m, ShouldResemble, msg)
				break loop
			}
		}
	})

	Convey("test host Send callback", t, func() {
		h.Send(ctx, msg, nodeID, localaddr)
		var m *raftpb.Message
		timer := time.NewTimer(3 * time.Second)

	loop:
		for {
			select {
			case m = <-ch:
				So(m, ShouldResemble, msg)
				break loop
			case <-timer.C:
				So(m, ShouldResemble, msg)
				break loop
			}
		}
	})

	Convey("test host Sendv callback", t, func() {
		h.Sendv(ctx, msgs, nodeID, localaddr)
		var m *raftpb.Message
		timer := time.NewTimer(3 * time.Second)
		i := 0
	loop:
		for {
			select {
			case m = <-ch:
				So(m, ShouldResemble, msgs[i])
				i++
				timer.Reset(3 * time.Second)
				if i == msgLen {
					break loop
				}
			case <-timer.C:
				So(m, ShouldResemble, msgs)
				break loop
			}
		}
	})

	Convey("test host resolveMultiplexer method", t, func() {
		h := h.(*host)
		testEndpoint := "127.0.0.1:11000"
		_, ok := h.peers.Load(testEndpoint)
		So(ok, ShouldBeFalse)
		p := h.resolveMultiplexer(ctx, nodeID, testEndpoint)
		So(p, ShouldNotBeNil)
		_, ok = h.peers.Load(testEndpoint)
		So(ok, ShouldBeTrue)
	})
}
