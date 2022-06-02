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
	"time"

	// third-party libraries.
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	vsraftpb "github.com/linkall-labs/vanus/proto/pkg/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"
)

const (
	defaultTimeoutMilliseconds = 300
	defaultMessageChainSize    = 32
)

type peer struct {
	addr   string
	msgc   chan *raftpb.Message
	stream vsraftpb.RaftServer_SendMsssageClient
	ctx    context.Context
	cancel context.CancelFunc
}

// Make sure peer implements Multiplexer.
var _ Multiplexer = (*peer)(nil)

func newPeer(ctx context.Context, endpoint string, callback string) *peer {
	ctx, cancel := context.WithCancel(ctx)

	p := &peer{
		addr:   endpoint,
		msgc:   make(chan *raftpb.Message, defaultMessageChainSize),
		ctx:    ctx,
		cancel: cancel,
	}

	go p.run(callback)

	return p
}

func (p *peer) run(callback string) {
	opts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	preface := raftpb.Message{
		Context: []byte(callback),
	}

loop:
	for {
		var err error
		select {
		case msg := <-p.msgc:
			stream := p.stream
			if stream == nil {
				if stream, err = p.connect(opts...); err != nil {
					p.processSendError(err)
					break
				}
				p.stream = stream
				if err = stream.Send(&preface); err != nil {
					p.processSendError(err)
					break
				}
			}
			if err = stream.Send(msg); err != nil {
				p.processSendError(err)
				break
			}
		case <-p.ctx.Done():
			break loop
		}
	}

	if p.stream != nil {
		_, _ = p.stream.CloseAndRecv()
	}
}

func (p *peer) processSendError(err error) {
	// TODO(james.yin): report MsgUnreachable, backoff
	if errors.Is(err, io.EOF) {
		_, _ = p.stream.CloseAndRecv()
		p.stream = nil
	}
}

func (p *peer) Close() {
	p.cancel()
}

func (p *peer) Send(msg *raftpb.Message) {
	// TODO(james.yin):
	select {
	case <-p.ctx.Done():
		return
	case p.msgc <- msg:
	}
}

func (p *peer) Sendv(msgs []*raftpb.Message) {
	for _, msg := range msgs {
		p.Send(msg)
	}
}

func (p *peer) connect(opts ...grpc.DialOption) (vsraftpb.RaftServer_SendMsssageClient, error) {
	timeout := defaultTimeoutMilliseconds * time.Millisecond
	ctx, cancel := context.WithTimeout(p.ctx, timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, p.addr, opts...)
	if err != nil {
		return nil, err
	}

	client := vsraftpb.NewRaftServerClient(conn)
	stream, err := client.SendMsssage(context.TODO())
	if err != nil {
		return nil, err
	}
	return stream, nil
}
