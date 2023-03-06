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
	"math"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/vanus-labs/vanus/raft"
	"github.com/vanus-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/executor"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

var (
	blockSize uint64 = 4 * 1024
	fileSize         = 8 * blockSize
	stateOpts        = []walog.Option{walog.WithFileSize(int64(fileSize))}
	hintOpts         = []walog.Option{walog.WithFileSize(int64(fileSize))}
	raftOpts         = []walog.Option{walog.WithFileSize(int64(fileSize))}
	nodeID1          = vanus.NewIDFromUint64(1)
	nodeID2          = vanus.NewIDFromUint64(2)
	nodeID3          = vanus.NewIDFromUint64(3)
)

func TestStorage(t *testing.T) {
	ctx := context.Background()

	stateDir := t.TempDir()
	hintDir := t.TempDir()
	raftDir := t.TempDir()

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

	Convey("raft storage", t, func() {
		stateStore, err := meta.RecoverSyncStore(ctx, stateDir, stateOpts...)
		So(err, ShouldBeNil)
		defer stateStore.Close(ctx)

		hintStore, err := meta.RecoverAsyncStore(ctx, hintDir, hintOpts...)
		So(err, ShouldBeNil)
		defer hintStore.Close()

		Convey("create raft log", func() {
			rawWAL, err := walog.Open(ctx, raftDir, walog.WithFileSize(int64(fileSize)))
			So(err, ShouldBeNil)
			wal := newWAL(rawWAL, stateStore)
			defer func() {
				wal.Close()
				wal.Wait()
			}()
			s, _ := NewStorage(ctx, nodeID1, wal, stateStore, hintStore, nil)

			hardSt, confSt, err := s.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{})
			So(confSt, ShouldResemble, raftpb.ConfState{})

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

			s.SetConfState(ctx, raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}}, stateCb)
			So(<-ch, ShouldBeNil)

			s.SetApplied(ctx, 1)

			ent = raftpb.Entry{Term: 2, Index: 2, Type: raftpb.EntryNormal}
			s.Append(ctx, []raftpb.Entry{ent}, appendCb)
			So(<-ch, ShouldBeNil)

			s.SetHardState(ctx, raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2}, stateCb)
			So(<-ch, ShouldBeNil)
		})

		Convey("recover raft storages", func() {
			storages, wal, err := Recover(ctx, raftDir, stateStore, hintStore, raftOpts...)
			So(err, ShouldBeNil)
			defer func() {
				wal.Close()
				wal.Wait()
			}()

			So(storages, ShouldHaveLength, 1)
			s, ok := storages[nodeID1]
			So(ok, ShouldBeTrue)

			fi, err := s.FirstIndex()
			So(err, ShouldBeNil)
			So(fi, ShouldEqual, 1)

			li, err := s.LastIndex()
			So(err, ShouldBeNil)
			So(li, ShouldEqual, 2)

			term1, err := s.Term(1)
			So(err, ShouldBeNil)
			So(term1, ShouldEqual, 1)

			term2, err := s.Term(2)
			So(err, ShouldBeNil)
			So(term2, ShouldEqual, 2)

			hardSt, confSt, err := s.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{Term: 2, Vote: nodeID1.Uint64(), Commit: 2})
			So(confSt, ShouldResemble, raftpb.ConfState{Voters: []uint64{nodeID1.Uint64()}})

			app := s.Applied()
			So(app, ShouldEqual, 1)

			ents, err := s.Entries(1, 3, 0)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 1)
			So(ents[0], ShouldResemble, raftpb.Entry{
				Term:   1,
				Index:  1,
				Type:   raftpb.EntryConfChange,
				Data:   data1,
				NodeId: nodeID1.Uint64(),
			})

			ents, err = s.Entries(1, 3, math.MaxUint64)
			So(err, ShouldBeNil)
			So(ents, ShouldHaveLength, 2)
			So(ents[1], ShouldResemble, raftpb.Entry{
				Term:     2,
				Index:    2,
				Type:     raftpb.EntryNormal,
				NodeId:   nodeID1.Uint64(),
				PrevTerm: 1,
			})

			_, err = s.Entries(3, 4, 0)
			So(err, ShouldEqual, raft.ErrUnavailable)

			Convey("add new members, and truncate", func() {
				s.AppendExecutor = &executor.InPlace{}
				ch := make(chan error, 1)
				appendCb := func(_ AppendResult, err error) {
					ch <- err
				}
				stateCb := func(err error) {
					ch <- err
				}

				s.Append(ctx, []raftpb.Entry{
					{Term: 3, Index: 3, Type: raftpb.EntryNormal},
				}, appendCb)
				So(<-ch, ShouldBeNil)

				s.SetHardState(ctx, raftpb.HardState{
					Term: 3, Vote: nodeID1.Uint64(), Commit: 3,
				}, stateCb)
				So(<-ch, ShouldBeNil)

				s.Append(ctx, []raftpb.Entry{{
					Term:  3,
					Index: 4,
					Type:  raftpb.EntryConfChange,
					Data:  data2,
				}}, appendCb)
				So(<-ch, ShouldBeNil)

				s.SetHardState(ctx, raftpb.HardState{
					Term: 3, Vote: nodeID1.Uint64(), Commit: 4,
				}, stateCb)
				So(<-ch, ShouldBeNil)

				s.SetConfState(ctx, raftpb.ConfState{
					Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
				}, stateCb)
				So(<-ch, ShouldBeNil)

				s.SetApplied(ctx, 4)

				s.Append(ctx, []raftpb.Entry{{
					Term:  3,
					Index: 5,
					Type:  raftpb.EntryNormal,
					Data:  []byte("hello world!"),
				}}, appendCb)
				So(<-ch, ShouldBeNil)

				s.Append(ctx, []raftpb.Entry{{
					Term:  4,
					Index: 5,
					Type:  raftpb.EntryNormal,
				}}, appendCb)
				So(<-ch, ShouldBeNil)

				s.SetHardState(ctx, raftpb.HardState{
					Term: 4, Vote: nodeID2.Uint64(), Commit: 5,
				}, stateCb)
				So(<-ch, ShouldBeNil)

				s.SetApplied(ctx, 5)

				s.Append(ctx, []raftpb.Entry{{
					Term:  4,
					Index: 6,
					Type:  raftpb.EntryNormal,
					Data:  []byte("nice job!"),
				}}, appendCb)
				So(<-ch, ShouldBeNil)

				s.SetHardState(ctx, raftpb.HardState{
					Term: 4, Vote: nodeID2.Uint64(), Commit: 6,
				}, stateCb)
				So(<-ch, ShouldBeNil)

				s.SetApplied(ctx, 6)
			})
		})

		Convey("recover raft log again", func() {
			storages, wal, err := Recover(ctx, raftDir, stateStore, hintStore, raftOpts...)
			So(err, ShouldBeNil)
			defer func() {
				wal.Close()
				wal.Wait()
			}()

			So(storages, ShouldHaveLength, 1)
			s, ok := storages[nodeID1]
			So(ok, ShouldBeTrue)

			fi, err := s.FirstIndex()
			So(err, ShouldBeNil)
			So(fi, ShouldEqual, 1)

			li, err := s.LastIndex()
			So(err, ShouldBeNil)
			So(li, ShouldEqual, 6)

			term3, err := s.Term(3)
			So(err, ShouldBeNil)
			So(term3, ShouldEqual, 3)

			term4, err := s.Term(4)
			So(err, ShouldBeNil)
			So(term4, ShouldEqual, 3)

			term5, err := s.Term(5)
			So(err, ShouldBeNil)
			So(term5, ShouldEqual, 4)

			term6, err := s.Term(6)
			So(err, ShouldBeNil)
			So(term6, ShouldEqual, 4)

			hardSt, confSt, err := s.InitialState()
			So(err, ShouldBeNil)
			So(hardSt, ShouldResemble, raftpb.HardState{Term: 4, Vote: nodeID2.Uint64(), Commit: 6})
			So(confSt, ShouldResemble, raftpb.ConfState{
				Voters: []uint64{nodeID1.Uint64(), nodeID2.Uint64(), nodeID3.Uint64()},
			})

			app := s.Applied()
			So(app, ShouldEqual, 6)

			ents, err := s.Entries(3, 7, math.MaxUint64)
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
