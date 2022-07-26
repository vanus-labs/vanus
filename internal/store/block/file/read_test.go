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

package file

import (
	// standard libraries.
	"context"
	"os"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
)

func TestRead(t *testing.T) {
	Convey("Create block ", t, func() {
		blockDir, err := os.MkdirTemp("", "block-*")
		So(err, ShouldBeNil)
		defer os.RemoveAll(blockDir)

		id := vanus.NewID()

		b, err := Create(context.Background(), blockDir, id, defaultCapacity)
		So(err, ShouldBeNil)

		Convey("Append entry", func() {
			actx := b.NewAppendContext(nil)
			So(actx, ShouldNotBeNil)

			payload := []byte("vanus")
			entry := block.Entry{
				Payload: payload,
			}

			entries, err2 := b.PrepareAppend(context.Background(), actx, entry)
			So(err2, ShouldBeNil)
			err2 = b.CommitAppend(context.Background(), entries...)
			So(err2, ShouldBeNil)

			Convey("Read entries", func() {
				entries, err3 := b.Read(context.Background(), 0, 2)
				So(err3, ShouldBeNil)
				So(len(entries), ShouldEqual, 1)
				So(entries[0].Payload, ShouldResemble, payload)

				_, err3 = b.Read(context.Background(), 1, 2)
				So(err3, ShouldBeError, block.ErrOffsetOnEnd)

				_, err3 = b.Read(context.Background(), 2, 2)
				So(err3, ShouldBeError, block.ErrOffsetExceeded)

				b.MarkFull(context.Background())
				_, err3 = b.Read(context.Background(), 1, 2)
				So(err3, ShouldBeError, block.ErrOffsetExceeded)
			})
		})

		Reset(func() {
			err = b.Close(context.Background())
			So(err, ShouldBeNil)
		})
	})
}
