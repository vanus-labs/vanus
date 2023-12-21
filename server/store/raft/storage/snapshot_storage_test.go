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

package storage

import (
	// standard libraries.
	"context"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
	. "go.uber.org/mock/gomock"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/raft/raftpb"

	// this project.
	"github.com/vanus-labs/vanus/lib/executor"
	"github.com/vanus-labs/vanus/server/store/meta"
	walog "github.com/vanus-labs/vanus/server/store/wal"
)

func TestStorage_SnapshotStorage(t *testing.T) {
	ctx := context.Background()

	metaDir := t.TempDir()
	offsetDir := t.TempDir()
	walDir := t.TempDir()

	cc1 := raftpb.ConfChange{
		Type: raftpb.ConfChangeAddNode, NodeID: nodeID1.Uint64(),
	}
	data1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	Convey("raft snapshot", t, func() {
		ctrl := NewController(t)
		defer ctrl.Finish()

		stateStore, err := meta.RecoverSyncStore(ctx, metaDir, stateOpts...)
		So(err, ShouldBeNil)
		defer stateStore.Close(ctx)

		hintStore, err := meta.RecoverAsyncStore(ctx, offsetDir, hintOpts...)
		So(err, ShouldBeNil)
		defer hintStore.Close()

		rawWAL, err := walog.Open(ctx, walDir, walog.WithFileSize(int64(fileSize)))
		So(err, ShouldBeNil)
		wal := newWAL(rawWAL, stateStore, true)
		defer wal.Close()

		snapOp := NewMockSnapshotOperator(ctrl)
		s, _ := NewStorage(ctx, nodeID1, wal, stateStore, hintStore, snapOp)

		s.AppendExecutor = &executor.InPlace{}
		ch := make(chan error, 1)
		appendCb := func(_ AppendResult, err error) {
			ch <- err
		}
		stateCb := func(err error) {
			ch <- err
		}

		ent := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1}
		s.Append(ctx, []raftpb.Entry{ent}, appendCb)
		So(<-ch, ShouldBeNil)

		ents, err := s.Entries(1, 2, 0)
		So(err, ShouldBeNil)
		So(ents, ShouldHaveLength, 1)
		So(ents[0], ShouldResemble, ent)

		s.SetHardState(ctx, raftpb.HardState{Term: 1, Commit: 1}, stateCb)
		So(<-ch, ShouldBeNil)

		confSt := raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}}
		s.SetConfState(ctx, confSt, stateCb)
		So(<-ch, ShouldBeNil)

		s.SetApplied(ctx, 1)

		ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
		s.Append(ctx, []raftpb.Entry{ent}, appendCb)
		So(<-ch, ShouldBeNil)

		s.SetHardState(ctx, raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2}, stateCb)
		So(<-ch, ShouldBeNil)

		s.SetApplied(ctx, 2)

		data := []byte("hello world!")
		s.Append(ctx, []raftpb.Entry{{
			Term:  2,
			Index: 3,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}}, appendCb)
		So(<-ch, ShouldBeNil)

		s.SetHardState(ctx, raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 3}, stateCb)
		So(<-ch, ShouldBeNil)

		s.SetApplied(ctx, 3)

		snapOp.EXPECT().GetSnapshot(Eq(uint64(3))).Return(data, nil)
		snap, err := s.Snapshot()
		So(err, ShouldBeNil)
		So(snap, ShouldResemble, raftpb.Snapshot{
			Data: data,
			Metadata: raftpb.SnapshotMetadata{
				Term:      2,
				Index:     3,
				ConfState: confSt,
			},
		})

		snapOp.EXPECT().ApplySnapshot(Eq(data)).Return(nil)
		s.ApplySnapshot(ctx, raftpb.Snapshot{
			Data: data,
			Metadata: raftpb.SnapshotMetadata{
				Term:  3,
				Index: 6,
				ConfState: raftpb.ConfState{
					Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
				},
			},
		})

		fi, err := s.FirstIndex()
		So(err, ShouldBeNil)
		So(fi, ShouldEqual, 7)

		li, err := s.LastIndex()
		So(err, ShouldBeNil)
		So(li, ShouldEqual, 6)

		term, err := s.Term(6)
		So(err, ShouldBeNil)
		So(term, ShouldEqual, 3)
	})
}
