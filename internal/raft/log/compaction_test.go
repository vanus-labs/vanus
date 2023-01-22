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
	"encoding/binary"
	"fmt"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// first-party libraries.
	"github.com/linkall-labs/vanus/raft/raftpb"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/meta"
	walog "github.com/linkall-labs/vanus/internal/store/wal"
)

func TestLog_Compact(t *testing.T) {
	data := make([]byte, blockSize)
	copy(data, []byte("hello world!"))

	metaDir := t.TempDir()
	offsetDir := t.TempDir()
	walDir := t.TempDir()

	Convey("raft log compaction", t, func() {
		metaStore, err := meta.RecoverSyncStore(stdCtx.Background(), metaCfg, metaDir)
		So(err, ShouldBeNil)
		defer metaStore.Close(stdCtx.Background())

		offsetStore, err := meta.RecoverAsyncStore(stdCtx.Background(), offsetCfg, offsetDir)
		So(err, ShouldBeNil)
		defer offsetStore.Close()

		rawWAL, err := walog.Open(stdCtx.Background(), walDir, walog.WithFileSize(int64(fileSize)))
		So(err, ShouldBeNil)
		wal := newWAL(rawWAL, metaStore)

		log1 := NewLog(nodeID1, wal, metaStore, offsetStore, nil)
		log2 := NewLog(nodeID2, wal, metaStore, offsetStore, nil)
		log3 := NewLog(nodeID3, wal, metaStore, offsetStore, nil)

		ch := make(chan error, 1)
		cb := func(_ AppendResult, err error) {
			ch <- err
		}

		data1, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID1.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		log1.Append(stdCtx.Background(), []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data1},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		data2, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID2.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		log2.Append(stdCtx.Background(), []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data2},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		data3, err := (&raftpb.ConfChange{
			Type: raftpb.ConfChangeAddNode, NodeID: nodeID3.Uint64(),
		}).Marshal()
		So(err, ShouldBeNil)
		log3.Append(stdCtx.Background(), []raftpb.Entry{
			{Term: 1, Index: 1, Type: raftpb.EntryConfChange, Data: data3},
			{Term: 2, Index: 2, Type: raftpb.EntryNormal},
		}, cb)
		So(<-ch, ShouldBeNil)

		err = log1.Compact(stdCtx.Background(), 2)
		So(err, ShouldBeNil)

		err = log2.Compact(stdCtx.Background(), 2)
		So(err, ShouldBeNil)

		log1.Append(stdCtx.Background(), []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		log2.Append(stdCtx.Background(), []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		log3.Append(stdCtx.Background(), []raftpb.Entry{{
			Term: 2, Index: 3, Type: raftpb.EntryNormal, Data: data,
		}}, cb)
		So(<-ch, ShouldBeNil)

		err = log3.Compact(stdCtx.Background(), 2)
		So(err, ShouldBeNil)

		err = log1.Compact(stdCtx.Background(), 3)
		So(err, ShouldBeNil)

		err = log3.Compact(stdCtx.Background(), 3)
		So(err, ShouldBeNil)

		wal.Close()
		wal.Wait()

		key1 := fmt.Sprintf("block/%020d/compact", nodeID1.Uint64())
		v1, ok := metaStore.Load([]byte(key1))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v1.([]byte)[0:8]), ShouldEqual, 3)
		So(binary.BigEndian.Uint64(v1.([]byte)[8:16]), ShouldEqual, 2)

		key2 := fmt.Sprintf("block/%020d/compact", nodeID2.Uint64())
		v2, ok := metaStore.Load([]byte(key2))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v2.([]byte)[0:8]), ShouldEqual, 2)
		So(binary.BigEndian.Uint64(v2.([]byte)[8:16]), ShouldEqual, 2)

		key3 := fmt.Sprintf("block/%020d/compact", nodeID3.Uint64())
		v3, ok := metaStore.Load([]byte(key3))
		So(ok, ShouldBeTrue)
		So(binary.BigEndian.Uint64(v3.([]byte)[0:8]), ShouldEqual, 3)
		So(binary.BigEndian.Uint64(v3.([]byte)[8:16]), ShouldEqual, 2)

		v, ok := metaStore.Load(walCompactKey)
		So(ok, ShouldBeTrue)
		So(v.(int64), ShouldEqual, log2.offs[1])
	})
}
