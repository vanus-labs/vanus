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
	"math"
	"os"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft"
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	storecfg "github.com/linkall-labs/vanus/internal/store"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

var (
	blockSize uint64 = 4 * 1024
	fileSize         = 8 * blockSize
	metaCfg          = storecfg.SyncStoreConfig{
		WAL: storecfg.WALConfig{
			FileSize: fileSize,
		},
	}
	offsetCfg = storecfg.AsyncStoreConfig{
		WAL: storecfg.WALConfig{
			FileSize: fileSize,
		},
	}
	raftCfg = storecfg.RaftConfig{
		WAL: storecfg.WALConfig{
			FileSize: fileSize,
		},
	}
	nodeID = vanus.NewIDFromUint64(1)
)

func TestLog(t *testing.T) {
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

	cc := raftpb.ConfChange{
		Type: raftpb.ConfChangeAddNode, NodeID: nodeID.Uint64(),
	}
	data, err := cc.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	Convey("raft log", t, func() {
		metaStore, err := meta.RecoverSyncStore(metaCfg, metaDir)
		So(err, ShouldBeNil)
		defer metaStore.Close()

		offsetStore, err := meta.RecoverAsyncStore(offsetCfg, offsetDir)
		So(err, ShouldBeNil)
		defer offsetStore.Close()

		Convey("create raft log", func() {
			rawWAL, err := walog.Open(walDir, walog.WithFileSize(int64(fileSize)))
			So(err, ShouldBeNil)
			wal := newWAL(rawWAL, metaStore)
			defer wal.Close()

			log := NewLog(nodeID, wal, metaStore, offsetStore)

			hardSt, confSt, err := log.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{})
			So(confSt, ShouldResemble, raftpb.ConfState{})

			ent := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data}
			err = log.Append([]raftpb.Entry{ent})
			So(err, ShouldBeNil)

			ents, err := log.Entries(1, 2, 0)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 1)
			So(ents[0], ShouldResemble, ent)

			err = log.SetHardState(raftpb.HardState{Term: 1, Commit: 1})
			So(err, ShouldBeNil)
			err = log.SetConfState(raftpb.ConfState{Voters: []uint64{nodeID.Uint64()}})
			So(err, ShouldBeNil)
			log.SetApplied(1)

			ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
			err = log.Append([]raftpb.Entry{ent})
			So(err, ShouldBeNil)

			err = log.SetHardState(raftpb.HardState{Term: 2, Vote: nodeID.Uint64(), Commit: 2})
			So(err, ShouldBeNil)
		})

		Convey("recover raft log", func() {
			logs, wal, err := RecoverLogsAndWAL(raftCfg, walDir, metaStore, offsetStore)
			So(err, ShouldBeNil)
			defer wal.Close()

			So(logs, ShouldHaveLength, 1)
			log, ok := logs[nodeID]
			So(ok, ShouldBeTrue)

			fi, err := log.FirstIndex()
			So(err, ShouldBeNil)
			So(fi, ShouldEqual, 1)

			li, err := log.LastIndex()
			So(err, ShouldBeNil)
			So(li, ShouldEqual, 2)

			term1, err := log.Term(1)
			So(err, ShouldBeNil)
			So(term1, ShouldEqual, 1)

			term2, err := log.Term(2)
			So(err, ShouldBeNil)
			So(term2, ShouldEqual, 2)

			hardSt, confSt, err := log.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{Term: 2, Vote: nodeID.Uint64(), Commit: 2})
			So(confSt, ShouldResemble, raftpb.ConfState{Voters: []uint64{nodeID.Uint64()}})

			app := log.Applied()
			So(app, ShouldEqual, 1)

			ents, err := log.Entries(1, 3, 0)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 1)
			So(ents[0], ShouldResemble, raftpb.Entry{
				Term:   1,
				Index:  1,
				Type:   raftpb.EntryConfChange,
				Data:   data,
				NodeId: nodeID.Uint64(),
			})

			ents, err = log.Entries(1, 3, math.MaxUint64)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 2)
			So(ents[1], ShouldResemble, raftpb.Entry{
				Term:     2,
				Index:    2,
				Type:     raftpb.EntryNormal,
				NodeId:   nodeID.Uint64(),
				PrevTerm: 1,
			})

			_, err = log.Entries(3, 4, 0)
			So(err, ShouldEqual, raft.ErrUnavailable)
		})
	})
}
