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
	"encoding/binary"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/executor"
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

func TestStorage_Compact(t *testing.T) {
	data := make([]byte, blockSize)
	copy(data, []byte("hello world!"))

	stateDir := t.TempDir()
	hintDir := t.TempDir()
	walDir := t.TempDir()

	Convey("raft storage compaction", t, func() {
		ctx := context.Background()

		stateStore, err := meta.RecoverSyncStore(ctx, stateDir, stateOpts...)
		So(err, ShouldBeNil)
		defer stateStore.Close(ctx)

		hintStore, err := meta.RecoverAsyncStore(ctx, hintDir, hintOpts...)
		So(err, ShouldBeNil)
		defer hintStore.Close()

		rawWAL, err := walog.Open(ctx, walDir, walog.WithFileSize(int64(fileSize)))
		So(err, ShouldBeNil)
		wal := newWAL(rawWAL, stateStore)

		s1, _ := NewStorage(ctx, nodeID1, wal, stateStore, hintStore, nil)
		s2, _ := NewStorage(ctx, nodeID2, wal, stateStore, hintStore, nil)
		s3, _ := NewStorage(ctx, nodeID3, wal, stateStore, hintStore, nil)

		s1.AppendExecutor = &executor.InPlace{}
		s2.AppendExecutor = &executor.InPlace{}
		s3.AppendExecutor = &executor.InPlace{}
		ch := make(chan error, 1)
		cb := func(_ AppendResult, err error) {
			ch <- err
		}

		data1, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID1.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		s1.Append(ctx, []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		data2, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID2.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		s2.Append(ctx, []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data2},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		data3, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID3.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		s3.Append(ctx, []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data3},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		err = s1.Compact(ctx, 2)
		So(err, ShouldBeNil)

		err = s2.Compact(ctx, 2)
		So(err, ShouldBeNil)

		s1.Append(ctx, []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		s2.Append(ctx, []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		s3.Append(ctx, []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		err = s3.Compact(ctx, 2)
		So(err, ShouldBeNil)

		err = s1.Compact(ctx, 3)
		So(err, ShouldBeNil)

		err = s3.Compact(ctx, 3)
		So(err, ShouldBeNil)

		wal.Close()
		wal.Wait()

		v1, ok := stateStore.Load([]byte(CompactKey(nodeID1.Uint64())))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v1.([]byte)[0:8]), ShouldEqual, 3)
		So(binary.BigEndian.Uint64(v1.([]byte)[8:16]), ShouldEqual, 2)

		v2, ok := stateStore.Load([]byte(CompactKey(nodeID2.Uint64())))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v2.([]byte)[0:8]), ShouldEqual, 2)
		So(binary.BigEndian.Uint64(v2.([]byte)[8:16]), ShouldEqual, 2)

		v3, ok := stateStore.Load([]byte(CompactKey(nodeID3.Uint64())))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v3.([]byte)[0:8]), ShouldEqual, 3)
		So(binary.BigEndian.Uint64(v3.([]byte)[8:16]), ShouldEqual, 2)

		v, ok := stateStore.Load(walCompactKey)
		So(ok, ShouldBeTrue)
		So(v.(int64), ShouldEqual, s2.offs[1])
	})
}
