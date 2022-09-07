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
	"errors"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

var ErrSnapshotOutOfOrder = errors.New("the snapshot is out of order")

type AppendContext interface {
	WriteOffset() int64
	Archived() bool
}

type TwoPCAppender interface {
	NewAppendContext(last Fragment) AppendContext
	PrepareAppend(ctx context.Context, appendCtx AppendContext, entries ...Entry) ([]int64, Fragment, bool, error)
	PrepareArchive(ctx context.Context, appendCtx AppendContext) (Fragment, error)
	CommitAppend(ctx context.Context, frags ...Fragment) (bool, error)
}

type Snapshoter interface {
	Snapshot(ctx context.Context) (Fragment, error)
	ApplySnapshot(ctx context.Context, snap Fragment) error
}

type Raw interface {
	Reader
	TwoPCAppender
	Snapshoter

	ID() vanus.ID

	Open(context.Context) error
	Close(context.Context) error
	Delete(context.Context) error
}

type Statistics struct {
	ID        vanus.ID
	Capacity  uint64
	Archived  bool
	EntryNum  uint32
	EntrySize uint64
	// FirstEntryStime is the millisecond timestamp when the first Entry will be written to Block.
	FirstEntryStime int64
	// LastEntryStime is the millisecond timestamp when the last Entry will be written to Block.
	LastEntryStime int64
}

type ArchivedListener interface {
	OnArchived(stat Statistics)
}

type ArchivedCallback func(stat Statistics)

// Make sure OnArchivedFunc implements ArchivedListener.
var _ ArchivedListener = (ArchivedCallback)(nil)

func (f ArchivedCallback) OnArchived(stat Statistics) {
	f(stat)
}
