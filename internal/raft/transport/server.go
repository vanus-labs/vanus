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
	// standard libraries
	"context"
	"io"

	// third-party libraries
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries
	raftpb "github.com/linkall-labs/vsproto/pkg/raft"
)

type server struct {
	dmx Demultiplexer
	ctx context.Context
}

// Make sure Server implements raftpb.RaftServerServer.
var _ raftpb.RaftServerServer = (*server)(nil)

func NewRaftServer(ctx context.Context, dmx Demultiplexer) raftpb.RaftServerServer {
	return &server{
		dmx: dmx,
		ctx: ctx,
	}
}

// SendMessage implements raftpb.RaftServerServer
func (s *server) SendMsssage(stream raftpb.RaftServer_SendMsssageServer) error {
	preface, err := stream.Recv()
	if err != nil {
		return err
	}

	callback := string(preface.Context)

	for {
		msg, err := stream.Recv()
		if err != nil {
			// close by client
			if err == io.EOF {
				return s.closeStream(stream)
			}
			return err
		}

		err = s.dmx.Receive(s.ctx, msg, callback)
		if err != nil {
			// server is closed
			if err == context.Canceled {
				return s.closeStream(stream)
			}

			return err
		}
	}
}

func (s *server) closeStream(stream raftpb.RaftServer_SendMsssageServer) error {
	empty := &emptypb.Empty{}
	return stream.SendAndClose(empty)
}
