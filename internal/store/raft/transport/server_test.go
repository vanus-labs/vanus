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
)

type receiver struct {
	recvch chan *raftpb.Message
}

func (r *receiver) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	r.recvch <- msg
}

var _ Receiver = (*receiver)(nil)

func TestServer(t *testing.T) {
	Convey("test server", t, func() {
		serverIP, serverPort := "127.0.0.1", 12050
		nodeID := uint64(2)

		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
		if err != nil {
			log.Error(context.Background(), "failed to listen", map[string]interface{}{
				"error": err,
			})
			os.Exit(-1)
		}
		// So(err, ShouldBeNil)
		receiveResolver := NewSimpleResolver()
		receiveHost := NewHost(receiveResolver, fmt.Sprintf("%s:%d", serverIP, serverPort))
		ch := make(chan *raftpb.Message, 15)
		r := &receiver{
			recvch: ch,
		}
		receiveHost.Register(nodeID, r)
		raftSrv := NewServer(receiveHost)
		srv := grpc.NewServer()
		RegisterRaftServerServer(srv, raftSrv)
		go func() {
			if err := srv.Serve(listener); err != nil {
				panic(err)
			}
		}()

		clientIP, clientPort := "127.0.0.1", 11900

		sendResolver := NewSimpleResolver()
		sendHost := NewHost(sendResolver, fmt.Sprintf("%s:%d", clientIP, clientPort))

		Convey("test Send", func() {
			msg := &raftpb.Message{
				To: nodeID,
			}
			timeoutCtx, cannel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cannel()

			sendHost.Send(timeoutCtx, msg, 100, fmt.Sprintf("%s:%d", serverIP, serverPort), func(err error) {})

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

		Reset(func() {
			sendHost.Stop()
			srv.GracefulStop()
		})
	})
}
