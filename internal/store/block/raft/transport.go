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

package raft

import (
	// standard libraries.
	"context"

	// first-party libraries.
	"github.com/linkall-labs/vanus/internal/raft/transport"
	"github.com/linkall-labs/vanus/raft/raftpb"
)

// Make sure appender implements transport.Receiver.
var _ transport.Receiver = (*appender)(nil)

func (r *appender) send(msgs []raftpb.Message) {
	if len(msgs) == 0 {
		return
	}

	if len(msgs) == 1 {
		msg := &msgs[0]
		endpoint := r.peerHint(msg.To)
		r.host.Send(r.ctx, msg, msg.To, endpoint, func(err error) {
			if err != nil {
				r.node.ReportUnreachable(msg.To)
			}
		})
		return
	}

	to := msgs[0].To
	for i := 1; i < len(msgs); i++ {
		if msgs[i].To != to {
			to = 0
			break
		}
	}

	// send to same node
	if to != 0 {
		endpoint := r.peerHint(to)
		for i := 0; i < len(msgs); i++ {
			r.host.Send(r.ctx, &msgs[i], to, endpoint, func(err error) {
				if err != nil {
					r.node.ReportUnreachable(to)
				}
			})
		}
		return
	}

	mm := make(map[uint64][]*raftpb.Message)
	for i := 0; i < len(msgs); i++ {
		msg := &msgs[i]
		mm[msg.To] = append(mm[msg.To], msg)
	}
	for to, msgs := range mm {
		endpoint := r.peerHint(to)
		for _, m := range msgs {
			r.host.Send(r.ctx, m, to, endpoint, func(err error) {
				if err != nil {
					r.node.ReportUnreachable(to)
				}
			})
		}
	}
}

func (r *appender) peerHint(to uint64) string {
	r.hintMu.RLock()
	defer r.hintMu.RUnlock()
	for i := range r.hint {
		p := &r.hint[i]
		if p.id == to {
			return p.endpoint
		}
	}
	return ""
}

// Receive implements transport.Receiver.
func (r *appender) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	if endpoint != "" {
		r.hintPeer(from, endpoint)
	}

	// TODO(james.yin): check ctx.Done().
	_ = r.node.Step(r.ctx, *msg)
}

func (r *appender) hintPeer(from uint64, endpoint string) {
	if endpoint == "" {
		return
	}

	// TODO(james.yin): optimize lock
	r.hintMu.Lock()
	defer r.hintMu.Unlock()
	p := func() *peer {
		for i := range r.hint {
			ep := &r.hint[i]
			if ep.id == from {
				return ep
			}
		}
		return nil
	}()
	if p == nil {
		r.hint = append(r.hint, peer{
			id:       from,
			endpoint: endpoint,
		})
	} else if p.endpoint != endpoint {
		p.endpoint = endpoint
	}
}
