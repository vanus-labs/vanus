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
	serverIP, serverPort := "127.0.0.1", 12040
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
	p := newPeer(fmt.Sprintf("%s:%d", serverIP, serverPort), "")

	defer func() {
		p.Close()
		srv.GracefulStop()
	}()

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

		err = stream.Send(msg)
		So(err, ShouldBeNil)
		stream.CloseAndRecv()
	})

	Convey("test peer Send", t, func() {
		msg := &raftpb.Message{
			To: nodeID,
		}

		timeoutCtx, cannel := context.WithTimeout(ctx, 3*time.Second)
		defer cannel()

		p.Send(timeoutCtx, msg)

		for i := 0; i < 3; i++ {
			select {
			case m := <-ch:
				So(m, ShouldResemble, msg)
				return
			default:
			}
			time.Sleep(50 * time.Millisecond)
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
		timeoutCtx, cannel := context.WithTimeout(ctx, 3*time.Second)
		defer cannel()
		p.Sendv(timeoutCtx, msgs)

		for i := 0; i < msgLen; i++ {
			for j := 0; j < 3; j++ {
				select {
				case m := <-ch:
					So(m, ShouldResemble, msgs[i])
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	})
}
