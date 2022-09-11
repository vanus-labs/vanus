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

package log

import (
	"context"
	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/meta"
)

type stateStorage struct {
	metaStore   *meta.SyncStore
	offsetStore *meta.AsyncStore

	prevHardSt raftpb.HardState
	prevConfSt raftpb.ConfState
	prevApply  uint64

	hsKey  []byte
	offKey []byte
	csKey  []byte
	appKey []byte
}

// InitialState returns the saved HardState and ConfState information.
func (l *Log) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return l.prevHardSt, l.prevConfSt, nil
}

// HardState returns the saved HardState.
//
// NOTE: This method is not thread-safty, it must be used in goroutine which call SetHardState!!!
func (l *Log) HardState() raftpb.HardState {
	return l.prevHardSt
}

// SetHardState saves the current HardState.
func (l *Log) SetHardState(ctx context.Context, hs raftpb.HardState) (err error) {
	if hs.Term != l.prevHardSt.Term || hs.Vote != l.prevHardSt.Vote {
		var data []byte
		if data, err = hs.Marshal(); err != nil {
			return err
		}
		l.metaStore.Store(ctx, l.hsKey, data)
		l.prevHardSt = hs
	} else {
		l.offsetStore.Store(l.offKey, hs.Commit)
		l.prevHardSt.Commit = hs.Commit
	}
	return nil
}

func (l *Log) SetConfState(ctx context.Context, cs raftpb.ConfState) error {
	data, err := cs.Marshal()
	if err != nil {
		return err
	}
	l.metaStore.Store(ctx, l.csKey, data)
	l.prevConfSt = cs
	return nil
}

func (l *Log) Applied() uint64 {
	return l.prevApply
}

func (l *Log) SetApplied(app uint64) {
	l.offsetStore.Store(l.appKey, app)
	l.prevApply = app
}
