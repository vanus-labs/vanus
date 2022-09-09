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
	stdCtx "context"
	// standard libraries.
	"os"
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
	metaDir, err := os.MkdirTemp("", "meta-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(metaDir)

	offsetDir, err := os.MkdirTemp("", "offset-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(offsetDir)

	walDir, err := os.MkdirTemp("", "wal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(walDir)

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

		metaStore, err := meta.RecoverSyncStore(stdCtx.Background(), metaCfg, metaDir)
		So(err, ShouldBeNil)
		defer metaStore.Close(stdCtx.Background())

		offsetStore, err := meta.RecoverAsyncStore(stdCtx.Background(), offsetCfg, offsetDir)
		So(err, ShouldBeNil)
		defer offsetStore.Close()

		rawWAL, err := walog.Open(stdCtx.Background(), walDir, walog.WithFileSize(int64(fileSize)))
		So(err, ShouldBeNil)
		wal := newWAL(rawWAL, metaStore)
		defer wal.Close()

		snapOp := NewMockSnapshotOperator(ctrl)
		log := NewLog(nodeID1, wal, metaStore, offsetStore, snapOp)

		ent := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1}
		err = log.Append(stdCtx.Background(), []raftpb.Entry{ent})
		So(err, ShouldBeNil)

		ents, err := log.Entries(1, 2, 0)
		So(err, ShouldBeNil)
		So(ents, ShouldHaveLength, 1)
		So(ents[0], ShouldResemble, ent)

		err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 1, Commit: 1})
		So(err, ShouldBeNil)

		confSt := raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}}
		err = log.SetConfState(stdCtx.Background(), confSt)
		So(err, ShouldBeNil)

		log.SetApplied(1)

		ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
		err = log.Append(stdCtx.Background(), []raftpb.Entry{ent})
		So(err, ShouldBeNil)

		err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2})
		So(err, ShouldBeNil)

		log.SetApplied(2)

		data := []byte("hello world!")
		err = log.Append(stdCtx.Background(), []raftpb.Entry{{
			Term:  2,
			Index: 3,
			Type:  raftpb.EntryNormal,
			Data:  data,
		}})
		So(err, ShouldBeNil)

		err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 3})
		So(err, ShouldBeNil)

		log.SetApplied(3)

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
		log.ApplySnapshot(stdCtx.Background(), raftpb.Snapshot{
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
