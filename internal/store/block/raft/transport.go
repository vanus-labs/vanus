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
	"github.com/linkall-labs/vanus/raft/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	// this project.
	"github.com/linkall-labs/vanus/internal/raft/transport"
)

// Make sure appender implements transport.Receiver.
var _ transport.Receiver = (*appender)(nil)

func (a *appender) send(ctx context.Context, msgs []raftpb.Message) {
	ctx, span := a.tracer.Start(ctx, "send", trace.WithAttributes(
		attribute.Int("message_count", len(msgs))))
	defer span.End()

	if len(msgs) == 0 {
		return
	}

	if len(msgs) == 1 {
		msg := &msgs[0]
		endpoint := a.peerHint(ctx, msg.To)
		a.host.Send(ctx, msg, msg.To, endpoint, func(err error) {
			if err != nil {
				a.node.ReportUnreachable(msg.To)
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
		endpoint := a.peerHint(ctx, to)
		for i := 0; i < len(msgs); i++ {
			a.host.Send(ctx, &msgs[i], to, endpoint, func(err error) {
				if err != nil {
					a.node.ReportUnreachable(to)
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
		endpoint := a.peerHint(ctx, to)
		for _, m := range msgs {
			a.host.Send(ctx, m, to, endpoint, func(err error) {
				if err != nil {
					a.node.ReportUnreachable(to)
				}
			})
		}
	}
}

func (a *appender) peerHint(ctx context.Context, to uint64) string {
	_, span := a.tracer.Start(ctx, "hintPeer")
	defer span.End()

	a.hintMu.RLock()
	defer a.hintMu.RUnlock()
	for i := range a.hint {
		p := &a.hint[i]
		if p.id == to {
			return p.endpoint
		}
	}
	return ""
}

// Receive implements transport.Receiver.
func (a *appender) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	ctx, span := a.tracer.Start(ctx, "Receive",
		trace.WithAttributes(attribute.Int64("node_id", int64(a.ID())), attribute.Int64("from", int64(from))))
	defer span.End()

	if endpoint != "" {
		a.hintPeer(ctx, from, endpoint)
	}

	_ = a.node.Step(ctx, *msg)
}

func (a *appender) hintPeer(ctx context.Context, from uint64, endpoint string) {
	_, span := a.tracer.Start(ctx, "hintPeer")
	defer span.End()

	if endpoint == "" {
		return
	}

	// TODO(james.yin): optimize lock
	a.hintMu.Lock()
	defer a.hintMu.Unlock()
	p := func() *peer {
		for i := range a.hint {
			ep := &a.hint[i]
			if ep.id == from {
				return ep
			}
		}
		return nil
	}()
	if p == nil {
		a.hint = append(a.hint, peer{
			id:       from,
			endpoint: endpoint,
		})
	} else if p.endpoint != endpoint {
		p.endpoint = endpoint
	}
}
