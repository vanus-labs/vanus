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

package meta

import (
	// standard libraries.
	"context"
	"os"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/config"
)

var (
	key0 = []byte("key0")
	key1 = []byte("key1")
	key2 = []byte("key2")
)

func TestSyncStore(t *testing.T) {
	Convey("SyncStore", t, func() {
		walDir, err := os.MkdirTemp("", "sync-*")
		So(err, ShouldBeNil)

		Convey("new empty SyncStore by recovery", func() {
			ss, err := RecoverSyncStore(context.Background(), config.SyncStore{}, walDir)

			So(err, ShouldBeNil)
			So(ss, ShouldNotBeNil)

			ss.Close(context.Background())
		})

		Convey("setup SyncStore", func() {
			ss, err := RecoverSyncStore(context.Background(), config.SyncStore{}, walDir)
			So(err, ShouldBeNil)
			ss.Store(context.Background(), key0, "value0")
			ss.Store(context.Background(), key1, "value1")
			ss.Close(context.Background())

			Convey("recover SyncStore", func() {
				ss, err = RecoverSyncStore(context.Background(), config.SyncStore{}, walDir)
				So(err, ShouldBeNil)

				value0, ok0 := ss.Load(key0)
				So(ok0, ShouldBeTrue)
				So(value0, ShouldResemble, "value0")

				value1, ok1 := ss.Load(key1)
				So(ok1, ShouldBeTrue)
				So(value1, ShouldResemble, "value1")

				_, ok2 := ss.Load(key2)
				So(ok2, ShouldBeFalse)

				Convey("modify SyncStore", func() {
					ss.Delete(context.Background(), key1)
					_, ok1 = ss.Load(key1)
					So(ok1, ShouldBeFalse)

					ss.Store(context.Background(), key2, "value2")
					value2, ok2 := ss.Load(key2)
					So(ok2, ShouldBeTrue)
					So(value2, ShouldResemble, "value2")

					ss.Close(context.Background())

					Convey("recover SyncStore again", func() {
						ss, err = RecoverSyncStore(context.Background(), config.SyncStore{}, walDir)
						So(err, ShouldBeNil)

						value0, ok0 := ss.Load(key0)
						So(ok0, ShouldBeTrue)
						So(value0, ShouldResemble, "value0")

						_, ok1 := ss.Load(key1)
						So(ok1, ShouldBeFalse)

						value2, ok2 := ss.Load(key2)
						So(ok2, ShouldBeTrue)
						So(value2, ShouldResemble, "value2")

						ss.Close(context.Background())
					})
				})
			})
		})

		Reset(func() {
			err := os.RemoveAll(walDir)
			So(err, ShouldBeNil)
		})
	})
}
