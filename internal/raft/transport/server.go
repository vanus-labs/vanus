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
	// standard libraries.
	"context"
	"errors"
	"io"

	// third-party libraries.
	emptypb "google.golang.org/protobuf/types/known/emptypb"

	// first-party libraries.
	raftpb "github.com/linkall-labs/vanus/proto/pkg/raft"
)

type server struct {
	dmx Demultiplexer
}

// Make sure server implements raftpb.RaftServerServer.
var _ raftpb.RaftServerServer = (*server)(nil)

func NewServer(dmx Demultiplexer) raftpb.RaftServerServer {
	return &server{
		dmx: dmx,
	}
}

// SendMessage implements raftpb.RaftServerServer.
func (s *server) SendMessage(stream raftpb.RaftServer_SendMessageServer) error {
	preface, err := stream.Recv()
	if err != nil {
		return err
	}

	callback := string(preface.Context)

	ctx := stream.Context()
	for {
		msg, err2 := stream.Recv()
		if err2 != nil {
			// close by client
			if errors.Is(err2, io.EOF) {
				return s.closeStream(stream)
			}
			return err2
		}

		err2 = s.dmx.Receive(ctx, msg, callback)
		if err2 != nil {
			// server is closed
			if errors.Is(err2, context.Canceled) {
				return s.closeStream(stream)
			}
			return err2
		}
	}
}

func (s *server) closeStream(stream raftpb.RaftServer_SendMessageServer) error {
	empty := &emptypb.Empty{}
	return stream.SendAndClose(empty)
}
