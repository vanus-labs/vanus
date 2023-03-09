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

package block

import (
	// standard libraries.
	"context"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/raft/transport"
)

// Make sure appender implements transport.Receiver.
var _ transport.Receiver = (*appender)(nil)

func (a *appender) send(ctx context.Context, msg *raftpb.Message) {
	to := msg.To
	endpoint := a.hint[to]
	a.host.Send(ctx, msg, to, endpoint, func(err error) {
		if err != nil {
			log.Warning(ctx, "send message failed", map[string]interface{}{
				log.KeyError: err,
				"to":         to,
				"endpoint":   endpoint,
			})
			a.reportUnreachable(msg.To)
		}
	})
}

// Receive implements transport.Receiver.
func (a *appender) Receive(ctx context.Context, msg *raftpb.Message, from uint64, endpoint string) {
	a.transportExecutor.Execute(func() {
		if endpoint != "" && a.hint[from] != endpoint {
			a.hint[from] = endpoint
			_ = a.e.RegisterNodeRecord(from, endpoint)
		}

		a.step(msg)
	})
}
