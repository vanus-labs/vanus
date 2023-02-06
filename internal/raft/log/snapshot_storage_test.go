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
	// standard libraries.
	"context"
	"testing"

	// third-party libraries.
	. "github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

func TestLog_SnapshotStorage(t *testing.T) {
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

		metaStore, err := meta.RecoverSyncStore(ctx, metaCfg, metaDir)
		So(err, ShouldBeNil)
		defer metaStore.Close(ctx)

		offsetStore, err := meta.RecoverAsyncStore(ctx, offsetCfg, offsetDir)
		So(err, ShouldBeNil)
		defer offsetStore.Close()

		rawWAL, err := walog.Open(ctx, walDir, walog.WithFileSize(int64(fileSize)))
		So(err, ShouldBeNil)
		wal := newWAL(rawWAL, metaStore)
		defer wal.Close()

		snapOp := NewMockSnapshotOperator(ctrl)
		log := NewLog(nodeID1, wal, metaStore, offsetStore, snapOp)

		ch := make(chan error, 1)
		cb := func(_ AppendResult, err error) {
			ch <- err
		}

		ent := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1}
		log.Append(ctx, []raftpb.Entry{ent}, cb)
		So(<-ch, ShouldBeNil)

		ents, err := log.Entries(1, 2, 0)
		So(err, ShouldBeNil)
		So(ents, ShouldHaveLength, 1)
		So(ents[0], ShouldResemble, ent)

		err = log.SetHardState(ctx, raftpb.HardState{Term: 1, Commit: 1})
		So(err, ShouldBeNil)

		confSt := raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}}
		err = log.SetConfState(ctx, confSt)
		So(err, ShouldBeNil)

		log.SetApplied(ctx, 1)

		ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
		log.Append(ctx, []raftpb.Entry{ent}, cb)
		So(<-ch, ShouldBeNil)

		err = log.SetHardState(ctx, raftpb.HardState{
			Term: 2, Vote: nodeID1.Uint64(), Commit: 2,
		})
		So(err, ShouldBeNil)

		log.SetApplied(ctx, 2)

		data := []byte("hello world!")
		log.Append(ctx, []raftpb.Entry{{
			Term:  2,
			Index: 3,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		err = log.SetHardState(ctx, raftpb.HardState{
			Term: 2, Vote: nodeID1.Uint64(), Commit: 3,
		})
		So(err, ShouldBeNil)

		log.SetApplied(ctx, 3)

		snapOp.EXPECT().GetSnapshot(Eq(uint64(3))).Return(data, nil)
		snap, err := log.Snapshot()
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
		log.ApplySnapshot(ctx, raftpb.Snapshot{
			Data: data,
			Metadata: raftpb.SnapshotMetadata{
				Term:  3,
				Index: 6,
				ConfState: raftpb.ConfState{
					Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
				},
			},
		})

		fi, err := log.FirstIndex()
		So(err, ShouldBeNil)
		So(fi, ShouldEqual, 7)

		li, err := log.LastIndex()
		So(err, ShouldBeNil)
		So(li, ShouldEqual, 6)

		term, err := log.Term(6)
		So(err, ShouldBeNil)
		So(term, ShouldEqual, 3)
	})
}
