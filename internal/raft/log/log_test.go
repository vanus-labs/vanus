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
	nodeID1 = vanus.NewIDFromUint64(1)
	nodeID2 = vanus.NewIDFromUint64(2)
	nodeID3 = vanus.NewIDFromUint64(3)
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

	cc1 := raftpb.ConfChange{
		Type: raftpb.ConfChangeAddNode, NodeID: nodeID1.Uint64(),
	}
	data1, err := cc1.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	cc2 := raftpb.ConfChangeV2{
		Changes: []raftpb.ConfChangeSingle{{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: nodeID2.Uint64(),
		}, {
			Type:   raftpb.ConfChangeAddNode,
			NodeID: nodeID3.Uint64(),
		}},
	}
	data2, err := cc2.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	Convey("raft log", t, func() {
		metaStore, err := meta.RecoverSyncStore(stdCtx.Background(), metaCfg, metaDir)
		So(err, ShouldBeNil)
		defer metaStore.Close(stdCtx.Background())

		offsetStore, err := meta.RecoverAsyncStore(stdCtx.Background(), offsetCfg, offsetDir)
		So(err, ShouldBeNil)
		defer offsetStore.Close()

		Convey("create raft log", func() {
			rawWAL, err := walog.Open(stdCtx.Background(), walDir, walog.WithFileSize(int64(fileSize)))
			So(err, ShouldBeNil)
			wal := newWAL(rawWAL, metaStore)
			defer func() {
				wal.Close()
				wal.Wait()
			}()
			log := NewLog(nodeID1, wal, metaStore, offsetStore, nil)

			hardSt, confSt, err := log.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{})
			So(confSt, ShouldResemble, raftpb.ConfState{})

			ent := raftpb.Entry{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1}
			err = log.Append(stdCtx.Background(), []raftpb.Entry{ent})
			So(err, ShouldBeNil)

			ents, err := log.Entries(1, 2, 0)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 1)
			So(ents[0], ShouldResemble, ent)

			err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 1, Commit: 1})
			So(err, ShouldBeNil)

			err = log.SetConfState(stdCtx.Background(), raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}})
			So(err, ShouldBeNil)

			log.SetApplied(1)

			ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
			err = log.Append(stdCtx.Background(), []raftpb.Entry{ent})
			So(err, ShouldBeNil)

			err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2})
			So(err, ShouldBeNil)
		})

		Convey("recover raft log", func() {
			logs, wal, err := RecoverLogsAndWAL(stdCtx.Background(), raftCfg, walDir, metaStore, offsetStore)
			So(err, ShouldBeNil)
			defer func() {
				wal.Close()
				wal.Wait()
			}()

			So(logs, ShouldHaveLength, 1)
			log, ok := logs[nodeID1]
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
			So(hardSt, ShouldResemble, raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2})
			So(confSt, ShouldResemble, raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}})

			app := log.Applied()
			So(app, ShouldEqual, 1)

			ents, err := log.Entries(1, 3, 0)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 1)
			So(ents[0], ShouldResemble, raftpb.Entry{
				Term:   1,
				Index:  1,
				Type:   raftpb.EntryConfChange,
				Data:   data1,
				NodeId: nodeID1.Uint64(),
			})

			ents, err = log.Entries(1, 3, math.MaxUint64)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 2)
			So(ents[1], ShouldResemble, raftpb.Entry{
				Term:     2,
				Index:    2,
				Type:     raftpb.EntryNormal,
				NodeId:   nodeID1.Uint64(),
				PrevTerm: 1,
			})

			_, err = log.Entries(3, 4, 0)
			So(err, ShouldEqual, raft.ErrUnavailable)

			Convey("add new members, and truncate", func() {
				err = log.Append(stdCtx.Background(), []raftpb.Entry{{Term: 3, Index: 3, Type: raftpb.EntryNormal}})
				So(err, ShouldBeNil)

				err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 3, Vote: nodeID1.Uint64(), Commit: 3})
				So(err, ShouldBeNil)

				err = log.Append(stdCtx.Background(), []raftpb.Entry{{
					Term:  3,
					Index: 4,
					Type:  raftpb.EntryConfChange,
					Data:  data2,
				}})
				So(err, ShouldBeNil)

				err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 3, Vote: nodeID1.Uint64(), Commit: 4})
				So(err, ShouldBeNil)

				err = log.SetConfState(stdCtx.Background(), raftpb.ConfState{
					Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
				})
				So(err, ShouldBeNil)

				log.SetApplied(4)

				err = log.Append(stdCtx.Background(), []raftpb.Entry{{
					Term:  3,
					Index: 5,
					Type:  raftpb.EntryNormal,
					Data:  []byte("hello world!"),
				}})
				So(err, ShouldBeNil)

				err = log.Append(stdCtx.Background(), []raftpb.Entry{{
					Term:  4,
					Index: 5,
					Type:  raftpb.EntryNormal,
				}})
				So(err, ShouldBeNil)

				err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 4, Vote: nodeID2.Uint64(), Commit: 5})
				So(err, ShouldBeNil)

				log.SetApplied(5)

				err = log.Append(stdCtx.Background(), []raftpb.Entry{{
					Term:  4,
					Index: 6,
					Type:  raftpb.EntryNormal,
					Data:  []byte("nice job!"),
				}})
				So(err, ShouldBeNil)

				err = log.SetHardState(stdCtx.Background(), raftpb.HardState{Term: 4, Vote: nodeID2.Uint64(), Commit: 6})
				So(err, ShouldBeNil)

				log.SetApplied(6)
			})
		})

		Convey("recover raft log again", func() {
			logs, wal, err := RecoverLogsAndWAL(stdCtx.Background(), raftCfg, walDir, metaStore, offsetStore)
			So(err, ShouldBeNil)
			defer func() {
				wal.Close()
				wal.Wait()
			}()

			So(logs, ShouldHaveLength, 1)
			log, ok := logs[nodeID1]
			So(ok, ShouldBeTrue)

			fi, err := log.FirstIndex()
			So(err, ShouldBeNil)
			So(fi, ShouldEqual, 1)

			li, err := log.LastIndex()
			So(err, ShouldBeNil)
			So(li, ShouldEqual, 6)

			term3, err := log.Term(3)
			So(err, ShouldBeNil)
			So(term3, ShouldEqual, 3)

			term4, err := log.Term(4)
			So(err, ShouldBeNil)
			So(term4, ShouldEqual, 3)

			term5, err := log.Term(5)
			So(err, ShouldBeNil)
			So(term5, ShouldEqual, 4)

			term6, err := log.Term(6)
			So(err, ShouldBeNil)
			So(term6, ShouldEqual, 4)

			hardSt, confSt, err := log.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{Term: 4, Vote: nodeID2.Uint64(), Commit: 6})
			So(confSt, ShouldResemble, raftpb.ConfState{
				Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
			})

			app := log.Applied()
			So(app, ShouldEqual, 6)

			ents, err := log.Entries(3, 7, math.MaxUint64)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 4)
			So(ents, ShouldResemble, []raftpb.Entry{{
				Term:     3,
				Index:    3,
				Type:     raftpb.EntryNormal,
				NodeId:   nodeID1.Uint64(),
				PrevTerm: 2,
			}, {
				Term:   3,
				Index:  4,
				Type:   raftpb.EntryConfChange,
				Data:   data2,
				NodeId: nodeID1.Uint64(),
			}, {
				Term:     4,
				Index:    5,
				Type:     raftpb.EntryNormal,
				NodeId:   nodeID1.Uint64(),
				PrevTerm: 3,
			}, {
				Term:   4,
				Index:  6,
				Type:   raftpb.EntryNormal,
				Data:   []byte("nice job!"),
				NodeId: nodeID1.Uint64(),
			}})
		})
	})
}
