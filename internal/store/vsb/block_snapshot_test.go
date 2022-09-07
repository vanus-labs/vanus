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

package vsb

// import (
// 	// standard libraries.
// 	"context"
// 	"os"
// 	"testing"

// 	// third-party libraries.
// 	. "github.com/smartystreets/goconvey/convey"

// 	// this project.
// 	"github.com/linkall-labs/vanus/internal/primitive/vanus"
// 	"github.com/linkall-labs/vanus/internal/store/block"
// )

// func TestVSBlock_SnapshotOperator(t *testing.T) {
// 	Convey("block snapshot", t, func() {
// 		blockDir, err := os.MkdirTemp("", "block-*")
// 		So(err, ShouldBeNil)
// 		defer os.RemoveAll(blockDir)

// 		b, err := Create(context.Background(), blockDir, vanus.NewID(), defaultCapacity)
// 		So(err, ShouldBeNil)

// 		snap, err := b.GetSnapshot(0)
// 		So(err, ShouldBeNil)
// 		So(snap, ShouldResemble, []byte{})

// 		actx := b.NewAppendContext(nil)
// 		So(actx, ShouldNotBeNil)

// 		payload := []byte("vanus")
// 		entries := []block.Entry{{Payload: payload}, {Payload: payload}, {Payload: payload}}

// 		var data []byte
// 		for i := range entries {
// 			entry := &entries[i]
// 			buf := make([]byte, entry.Size())
// 			_, err2 := entries[i].MarshalTo(buf)
// 			So(err2, ShouldBeNil)
// 			data = append(data, buf...)
// 		}

// 		entries, err = b.PrepareAppend(context.Background(), actx, entries...)
// 		So(err, ShouldBeNil)
// 		err = b.CommitAppend(context.Background(), entries...)
// 		So(err, ShouldBeNil)

// 		snap, err = b.GetSnapshot(0)
// 		So(err, ShouldBeNil)
// 		So(snap, ShouldResemble, data)

// 		ent := block.Entry{
// 			Offset:  actx.WriteOffset(),
// 			Index:   3,
// 			Payload: payload,
// 		}
// 		buf := make([]byte, ent.Size())
// 		_, err = ent.MarshalTo(buf)
// 		So(err, ShouldBeNil)
// 		data = append(data, buf...)

// 		err = b.ApplySnapshot(data)
// 		So(err, ShouldBeNil)

// 		entries, err = b.Read(context.Background(), 3, 1)
// 		So(err, ShouldBeNil)
// 		So(entries, ShouldHaveLength, 1)
// 		So(entries[0], ShouldResemble, ent)

// 		err = b.Close(context.Background())
// 		So(err, ShouldBeNil)
// 	})
// }
