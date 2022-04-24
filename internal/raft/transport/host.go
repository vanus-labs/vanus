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
	"sync"

	// third-party libraries
	"github.com/linkall-labs/raft/raftpb"
)

type Host interface {
	Sender
	Demultiplexer

	Register(node uint64, r Receiver)
}

type host struct {
	peers     sync.Map
	receivers sync.Map
	resolver  Resolver
	callback  string
}

// Make sure host implements Host.
var _ Host = (*host)(nil)

func NewHost(resolver Resolver, callback string) Host {
	h := &host{
		resolver: resolver,
		callback: callback,
	}
	return h
}

func (h *host) Send(ctx context.Context, msg *raftpb.Message, to uint64, endpoint string) {
	mux := h.resolveMultiplexer(ctx, to, endpoint)
	if mux == nil {
		return
	}
	mux.Send(msg)
}

func (h *host) Sendv(ctx context.Context, msgs []*raftpb.Message, to uint64, endpoint string) {
	mux := h.resolveMultiplexer(ctx, to, endpoint)
	if mux == nil {
		return
	}
	mux.Sendv(msgs)
}

func (h *host) resolveMultiplexer(ctx context.Context, to uint64, endpoint string) Multiplexer {
	if endpoint == "" {
		if endpoint = h.resolver.Resolve(to); endpoint == "" {
			return nil
		}
	}
	if mux, ok := h.peers.Load(endpoint); ok {
		return mux.(*peer)
	}
	// TODO(james.yin): clean unused peer
	p := newPeer(context.TODO(), endpoint, h.callback)
	if mux, loaded := h.peers.LoadOrStore(endpoint, p); loaded {
		defer p.Close()
		return mux.(*peer)
	}
	return p
}

// Receive implements Demultiplexer
func (h *host) Receive(ctx context.Context, msg *raftpb.Message, endpoint string) error {
	if receiver, ok := h.receivers.Load(msg.To); ok {
		receiver.(Receiver).Receive(ctx, msg, msg.From, endpoint)
	}
	return nil
}

func (h *host) Register(node uint64, r Receiver) {
	// TODO(james.yin): Handles the case where the receiver already exists.
	h.receivers.LoadOrStore(node, r)
}
