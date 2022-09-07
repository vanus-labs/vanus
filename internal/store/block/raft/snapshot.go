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

	// this project.
	raftlog "github.com/linkall-labs/vanus/internal/raft/log"
	"github.com/linkall-labs/vanus/internal/store/block"
)

// Make sure appender implements raftlog.SnapshotOperator.
var _ raftlog.SnapshotOperator = (*appender)(nil)

func (r *appender) GetSnapshot(index uint64) ([]byte, error) {
	snap, err := r.raw.Snapshot(context.Background())
	if err != nil {
		return nil, err
	}
	data, err := block.MarshalFragment(snap)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *appender) ApplySnapshot(data []byte) error {
	snap := block.NewFragment(data)
	return r.raw.ApplySnapshot(context.Background(), snap)
}
