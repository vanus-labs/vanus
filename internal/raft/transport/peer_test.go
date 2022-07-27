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
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/observability/log"
	. "github.com/linkall-labs/vanus/proto/pkg/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestPeer(t *testing.T) {
	serverIP, serverPort := "127.0.0.1", 12000
	nodeID := uint64(2)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		log.Error(context.Background(), "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}
	receiveResolver := NewSimpleResolver()
	receiveHost := NewHost(receiveResolver, fmt.Sprintf("%s:%d", serverIP, serverPort))
	ch := make(chan *raftpb.Message, 10)
	receiveHost.Register(nodeID, &receiver{
		recvch: ch,
	})
	raftSrv := NewServer(receiveHost)
	srv := grpc.NewServer()
	RegisterRaftServerServer(srv, raftSrv)
	go func() {
		if err := srv.Serve(listener); err != nil {
			panic(err)
		}
	}()
	ctx := context.Background()
	p := newPeer(fmt.Sprintf("%s:%d", serverIP, serverPort), " ")
	Convey("test peer connect", t, func() {
		opts := []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		ctx := context.Background()
		stream, err := p.connect(ctx, opts...)
		So(stream, ShouldNotBeNil)
		So(err, ShouldBeNil)
		msg := &raftpb.Message{
			To: nodeID,
		}
		Convey("test stream send", func() {
			err = stream.Send(msg)
			So(err, ShouldBeNil)
		})
	})

	Convey("test peer Send", t, func() {
		msg := &raftpb.Message{
			To: nodeID,
		}
		p.Send(ctx, msg)
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

	Convey("test peer Sendv", t, func() {
		msgLen := 5
		msgs := make([]*raftpb.Message, msgLen)
		for i := 0; i < msgLen; i++ {
			msgs[i] = &raftpb.Message{
				To: nodeID,
			}
		}
		p.Sendv(ctx, msgs)
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
}
