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

package raft

import (
	pb "github.com/vanus-labs/vanus/raft/raftpb"
)

type Keeper interface {
	StateKeeper
	LogKeeper
	AppKeeper
	NetKeeper
}

type StateKeeper interface {
	SetHardState(st pb.HardState)
	SetSoftState(st SoftState)
	CommitTo(index uint64)
}

type LogKeeper interface {
	TruncateAndAppend(ents []pb.Entry)
	CompactTo(index uint64)
}

type AppKeeper interface {
	Apply(ents []pb.Entry)
}

type NetKeeper interface {
	Send(msg pb.Message)
}
