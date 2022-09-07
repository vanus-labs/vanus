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
	"sync"

	// third-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"
)

type SendCallback func(error)

type Host interface {
	Sender
	Demultiplexer

	Stop()
	Register(node uint64, r Receiver)
	// TODO(james.yin): Unregister
}

type host struct {
	peers     sync.Map
	receivers sync.Map
	resolver  Resolver
	callback  string
	lo        Multiplexer
}

// Make sure host implements Host.
var _ Host = (*host)(nil)

func NewHost(resolver Resolver, callback string) Host {
	h := &host{
		resolver: resolver,
		callback: callback,
	}
	h.lo = &loopback{
		addr: callback,
		dmu:  h,
	}
	return h
}

func (h *host) Stop() {
	h.peers.Range(func(key, value interface{}) bool {
		p, _ := value.(*peer)
		p.Close()
		return true
	})
}

func (h *host) Send(ctx context.Context, msg *raftpb.Message, to uint64, endpoint string, cb SendCallback) {
	mux := h.resolveMultiplexer(ctx, to, endpoint)
	if mux == nil {
		cb(ErrNotReachable)
		return
	}
	mux.Send(ctx, msg, cb)
}

func (h *host) resolveMultiplexer(ctx context.Context, to uint64, endpoint string) Multiplexer {
	if endpoint == "" {
		if endpoint = h.resolver.Resolve(to); endpoint == "" {
			return nil
		}
	}

	if endpoint == h.callback {
		return h.lo
	}

	if mux, ok := h.peers.Load(endpoint); ok {
		p, _ := mux.(*peer)
		return p
	}
	p := newPeer(endpoint, h.callback)
	if mux, loaded := h.peers.LoadOrStore(endpoint, p); loaded {
		defer p.Close()
		p2, _ := mux.(*peer)
		return p2
	}
	return p
}

// Receive implements Demultiplexer.
func (h *host) Receive(ctx context.Context, msg *raftpb.Message, endpoint string) error {
	if receiver, ok := h.receivers.Load(msg.To); ok {
		r, _ := receiver.(Receiver)
		r.Receive(ctx, msg, msg.From, endpoint)
	}
	return nil
}

func (h *host) Register(node uint64, r Receiver) {
	// TODO(james.yin): Handles the case where the receiver already exists.
	h.receivers.LoadOrStore(node, r)
}
