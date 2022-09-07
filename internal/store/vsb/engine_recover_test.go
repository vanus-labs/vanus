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
// 	"github.com/linkall-labs/vanus/internal/primitive/vanus"
// 	. "github.com/smartystreets/goconvey/convey"
// )

// func TestRecover(t *testing.T) {
// 	Convey("block recovery", t, func() {
// 		blockDir, err := os.MkdirTemp("", "block-*")
// 		So(err, ShouldBeNil)

// 		Convey("recover with empty dir", func() {
// 			blocks, err := Recover(blockDir)
// 			So(err, ShouldBeNil)
// 			So(len(blocks), ShouldEqual, 0)
// 		})

// 		Convey("create block", func() {
// 			id := vanus.NewID()
// 			path := resolvePath(blockDir, id)

// 			b, err := Create(context.Background(), blockDir, id, defaultCapacity)
// 			So(err, ShouldBeNil)

// 			err = b.Close(context.Background())
// 			So(err, ShouldBeNil)

// 			Convey("recover block", func() {
// 				blocks, err := Recover(blockDir)
// 				So(err, ShouldBeNil)
// 				So(len(blocks), ShouldEqual, 1)

// 				b, ok := blocks[id]
// 				So(ok, ShouldBeTrue)

// 				info, err := os.Stat(path)
// 				So(err, ShouldBeNil)
// 				So(info.Size(), ShouldEqual, defaultCapacity)

// 				So(b.ID(), ShouldEqual, id)
// 				So(b.Path(), ShouldEqual, path)
// 				So(b.full(), ShouldBeFalse)
// 				So(b.Appendable(), ShouldBeTrue)
// 				So(b.size(), ShouldEqual, 0)

// 				err = b.Close(context.Background())
// 				So(err, ShouldBeNil)
// 			})
// 		})

// 		Reset(func() {
// 			err := os.RemoveAll(blockDir)
// 			So(err, ShouldBeNil)
// 		})
// 	})
// }
