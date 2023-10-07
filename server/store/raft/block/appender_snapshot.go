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

	// this project.
	"github.com/vanus-labs/vanus/server/store/block"
	"github.com/vanus-labs/vanus/server/store/raft/storage"
)

// Make sure appender implements storage.SnapshotOperator.
var _ storage.SnapshotOperator = (*appender)(nil)

func (a *appender) GetSnapshot(_ uint64) ([]byte, error) {
	ctx := context.Background()
	snap, err := a.raw.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	data, err := block.MarshalFragment(snap)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (a *appender) ApplySnapshot(data []byte) error {
	snap := block.NewFragment(data)
	return a.raw.ApplySnapshot(context.Background(), snap)
}
