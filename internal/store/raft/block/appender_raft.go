// Copyright 2023 Linkall Inc.
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
	"github.com/vanus-labs/vanus/raft"
	"github.com/vanus-labs/vanus/raft/raftpb"
)

func (a *appender) step(msg *raftpb.Message) {
	a.raftExecutor.Execute(func() {
		_ = a.node.Step(*msg)
	})
}

func (a *appender) propose(pds ...raft.ProposeData) {
	a.raftExecutor.Execute(func() {
		a.node.Propose(pds...)
	})
}

func (a *appender) reportStateStatus(_ context.Context, term, vote uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportStateStatus(term, vote)
	})
}

func (a *appender) reportLogStatus(_ context.Context, index, term uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportLogStatus(index, term)
	})
}

func (a *appender) reportApplyStatus(_ context.Context, index uint64) {
	a.raftExecutor.Execute(func() {
		_ = a.node.ReportApplyStatus(index)
	})
}

func (a *appender) reportUnreachable(id uint64) {
	a.raftExecutor.Execute(func() {
		a.node.ReportUnreachable(id)
	})
}

func (a *appender) tick() bool {
	return a.raftExecutor.Execute(func() {
		a.node.Tick()
	})
}

func (a *appender) bootstrap(peers []raft.Peer) error {
	ch := make(chan error, 1)
	ok := a.raftExecutor.Execute(func() {
		ch <- a.node.Bootstrap(peers)
	})
	if !ok {
		return raft.ErrStopped
	}
	// FIXME(james.yin): appender is stopped when bootstrap.
	return <-ch
}

func (a *appender) applyConfChange(cc raftpb.ConfChangeI) *raftpb.ConfState {
	ch := make(chan *raftpb.ConfState, 1)
	a.raftExecutor.Execute(func() {
		ch <- a.node.ApplyConfChange(cc)
	})
	return <-ch
}
