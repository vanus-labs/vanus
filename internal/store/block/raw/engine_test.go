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

package raw

// import (
// 	// standard libraries.
// 	"context"
// 	"os"
// 	"testing"
// 	"time"

// 	// third-party libraries.
// 	. "github.com/smartystreets/goconvey/convey"

// 	// this project.
// 	"github.com/linkall-labs/vanus/internal/primitive/vanus"
// 	"github.com/linkall-labs/vanus/internal/store/block"
// )

// const (
// 	defaultCapacity = 4 * 1024 * 1024
// )

// func TestCreate(t *testing.T) {
// 	Convey("Create block ", t, func() {
// 		blockDir, err := os.MkdirTemp("", "block-*")
// 		So(err, ShouldBeNil)
// 		defer os.RemoveAll(blockDir)

// 		id := vanus.NewID()
// 		path := resolvePath(blockDir, id)

// 		b, err := Create(context.Background(), blockDir, id, defaultCapacity)
// 		So(err, ShouldBeNil)

// 		info, err := os.Stat(path)
// 		So(err, ShouldBeNil)
// 		So(info.Size(), ShouldEqual, defaultCapacity)

// 		So(b.ID(), ShouldEqual, id)
// 		So(b.Path(), ShouldEqual, path)
// 		So(b.full(), ShouldBeFalse)
// 		So(b.Appendable(), ShouldBeTrue)
// 		So(b.size(), ShouldEqual, 0)

// 		err = b.Close(context.Background())
// 		So(err, ShouldBeNil)
// 	})
// }

// func TestOpen(t *testing.T) {
// 	Convey("Create block ", t, func() {
// 		blockDir, err := os.MkdirTemp("", "block-*")
// 		So(err, ShouldBeNil)
// 		defer os.RemoveAll(blockDir)

// 		id := vanus.NewID()
// 		path := resolvePath(blockDir, id)

// 		b, err := Create(context.Background(), blockDir, id, defaultCapacity)
// 		So(err, ShouldBeNil)

// 		Convey("Append entry, then close the block", func() {
// 			actx := b.NewAppendContext(nil)
// 			So(actx, ShouldNotBeNil)

// 			payload := []byte("vanus")
// 			entry := block.Entry{
// 				Payload: payload,
// 			}

// 			entries, err := b.PrepareAppend(context.Background(), actx, entry)
// 			So(err, ShouldBeNil)
// 			err = b.CommitAppend(context.Background(), entries...)
// 			So(err, ShouldBeNil)

// 			sz := b.size()

// 			err = b.Close(context.Background())
// 			So(err, ShouldBeNil)

// 			Convey("Open exist block", func() {
// 				b, err := Open(context.Background(), path)
// 				So(err, ShouldBeNil)

// 				So(b.ID(), ShouldEqual, id)
// 				So(b.Path(), ShouldEqual, path)
// 				So(b.full(), ShouldBeFalse)
// 				So(b.Appendable(), ShouldBeTrue)
// 				So(b.size(), ShouldEqual, sz)

// 				entries, err := b.Read(context.Background(), 0, 2)
// 				So(err, ShouldBeNil)
// 				So(len(entries), ShouldEqual, 1)
// 				So(entries[0].Payload, ShouldResemble, payload)

// 				Convey("Mark the block full, then close the block", func() {
// 					err := b.MarkFull(context.Background())
// 					So(err, ShouldBeNil)

// 					time.Sleep(100 * time.Millisecond)

// 					err = b.Close(context.Background())
// 					So(err, ShouldBeNil)

// 					Convey("Open full block", func() {
// 						b, err := Open(context.Background(), path)
// 						So(err, ShouldBeNil)

// 						So(b.full(), ShouldBeTrue)
// 						So(b.Appendable(), ShouldBeFalse)
// 						So(b.size(), ShouldEqual, sz)

// 						entries, err := b.Read(context.Background(), 0, 2)
// 						So(err, ShouldBeNil)
// 						So(len(entries), ShouldEqual, 1)
// 						So(entries[0].Payload, ShouldResemble, payload)
// 					})
// 				})
// 			})
// 		})
// 	})
// }
