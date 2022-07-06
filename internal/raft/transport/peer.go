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
	defaultConnectTimeout   = 300 * time.Millisecond
	defaultMessageChainSize = 32
)

type messageWithContext struct {
	msg *raftpb.Message
	ctx context.Context
}

type peer struct {
	addr   string
	msgc   chan messageWithContext
	stream vsraftpb.RaftServer_SendMessageClient
	closec chan struct{}
	donec  chan struct{}
}

// Make sure peer implements Multiplexer.
var _ Multiplexer = (*peer)(nil)

func newPeer(endpoint string, callback string) *peer {
	p := &peer{
		addr:   endpoint,
		msgc:   make(chan messageWithContext, defaultMessageChainSize),
		closec: make(chan struct{}),
		donec:  make(chan struct{}),
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
		case mwc := <-p.msgc:
			stream := p.stream
			if stream == nil {
				if stream, err = p.connect(mwc.ctx, opts...); err != nil {
					p.processSendError(err)
					break
				}
				p.stream = stream
				if err = stream.Send(&preface); err != nil {
					p.processSendError(err)
					break
				}
			}
			if err = stream.Send(mwc.msg); err != nil {
				p.processSendError(err)
				break
			}
		case <-p.closec:
			break loop
		}
	}

	if p.stream != nil {
		_, _ = p.stream.CloseAndRecv()
	}

	close(p.donec)
}

func (p *peer) processSendError(err error) {
	// TODO(james.yin): report MsgUnreachable, backoff
	if errors.Is(err, io.EOF) {
		_, _ = p.stream.CloseAndRecv()
		p.stream = nil
	}
}

func (p *peer) Close() {
	close(p.closec)
	<-p.donec
}

func (p *peer) Send(ctx context.Context, msg *raftpb.Message) {
	mwc := messageWithContext{
		msg: msg,
		ctx: ctx,
	}

	select {
	case <-ctx.Done():
		return
	case <-p.closec:
		return
	case p.msgc <- mwc:
	}
}

func (p *peer) Sendv(ctx context.Context, msgs []*raftpb.Message) {
	for _, msg := range msgs {
		p.Send(ctx, msg)
	}
}

func (p *peer) connect(ctx context.Context, opts ...grpc.DialOption) (vsraftpb.RaftServer_SendMessageClient, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultConnectTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, p.addr, opts...)
	if err != nil {
		return nil, err
	}

	client := vsraftpb.NewRaftServerClient(conn)
	stream, err := client.SendMessage(context.TODO())
	if err != nil {
		return nil, err
	}
	return stream, nil
}
