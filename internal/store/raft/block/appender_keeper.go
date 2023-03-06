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

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

// Make sure appender implements raft.Keeper.
var _ raft.Keeper = (*appender)(nil)

func (a *appender) SetHardState(st raftpb.HardState) {
	a.commitExecutor.Execute(func() {
		a.persistHardState(context.TODO(), st)
	})
}

func (a *appender) CommitTo(index uint64) {
	a.commitExecutor.Execute(func() {
		a.storage.SetCommit(context.TODO(), index)
	})
}

func (a *appender) SetSoftState(st raft.SoftState) {
	// TODO(james.yin): dispatch to another goroutine.
	a.leaderID = vanus.NewIDFromUint64(st.Lead)
	if st.RaftState == raft.StateLeader {
		a.becomeLeader(context.TODO())
	}
}

func (a *appender) TruncateAndAppend(ents []raftpb.Entry) {
	a.persistExecutor.Execute(func() {
		a.persistEntries(context.TODO(), ents)
	})
}

func (a *appender) CompactTo(index uint64) {
	a.persistExecutor.Execute(func() {
		a.compactLog(context.TODO(), index)
	})
}

func (a *appender) Apply(ents []raftpb.Entry) {
	a.applyExecutor.Execute(func() {
		a.applyEntries(context.TODO(), ents)
	})
}

func (a *appender) Send(msg raftpb.Message) {
	a.transportExecutor.Execute(func() {
		ctx, cancel := context.WithTimeout(context.TODO(), defaultSendTimeout)
		defer cancel()
		a.send(ctx, &msg)
	})
}
