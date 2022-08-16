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

type dmu struct {
	recvch chan *raftpb.Message
}

func (d *dmu) Receive(ctx context.Context, msg *raftpb.Message, endpoint string) error {
	d.recvch <- msg
	return nil
}

var _ Demultiplexer = (*dmu)(nil)

func TestLoopBack(t *testing.T) {
	Convey("test loopback", t, func() {
		ch := make(chan *raftpb.Message, 15)
		loopbackInstance := loopback{
			addr: "127.0.0.1:12000",
			dmu: &dmu{
				recvch: ch,
			},
		}

		Convey("test loopback Send method", func() {
			msg := &raftpb.Message{
				To: 2,
			}
			timeoutCtx, cannel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cannel()

			loopbackInstance.Send(timeoutCtx, msg, func(err error) {})
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
	})
}
