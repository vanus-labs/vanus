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

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"
)

type loopback struct {
	addr string
	dmu  Demultiplexer
}

var _ Multiplexer = (*loopback)(nil)

func (lo *loopback) Send(ctx context.Context, msg *raftpb.Message, cb SendCallback) {
	cb(nil)
	_ = lo.dmu.Receive(ctx, msg, lo.addr)
}
