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

package store

import (
	// standard libraries.
	"os"
	"runtime"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"
)

func TestWAL_AppendOne(t *testing.T) {
	Convey("store configuration", t, func() {
		f, err := os.CreateTemp("", "store-*.yaml")
		So(err, ShouldBeNil)
		defer os.Remove(f.Name())

		_, err = f.WriteString(`controllers:
  - "127.0.0.1:2048"
ip: "127.0.0.1"
port: 11811
volume:
  id: 1
  dir: /linkall/volume/store-1
  capacity: 536870912
meta_store:
  wal:
    file_size: 4194304
    io:
      engine: psync
#offset_store:
#  wal:
#    io:
#      engine: psync
raft:
  wal:
    io:
      engine: io_uring
`)
		So(err, ShouldBeNil)

		err = f.Close()
		So(err, ShouldBeNil)

		cfg, err := InitConfig(f.Name())
		So(err, ShouldBeNil)

		So(cfg.ControllerAddresses, ShouldResemble, []string{"127.0.0.1:2048"})
		So(cfg.IP, ShouldEqual, "127.0.0.1")
		So(cfg.Port, ShouldEqual, 11811)

		So(cfg.Volume.ID, ShouldEqual, 1)
		So(cfg.Volume.Dir, ShouldEqual, "/linkall/volume/store-1")
		So(cfg.Volume.Capacity, ShouldEqual, 536870912)

		So(cfg.MetaStore.WAL.FileSize, ShouldEqual, 4194304)
		So(cfg.MetaStore.WAL.IO.Engine, ShouldEqual, "psync")
		So(len(cfg.MetaStore.WAL.Options()), ShouldEqual, 2)

		So(cfg.OffsetStore.WAL.IO.Engine, ShouldEqual, "")
		So(len(cfg.OffsetStore.WAL.Options()), ShouldEqual, 0)

		So(cfg.Raft.WAL.IO.Engine, ShouldEqual, "io_uring")
		if runtime.GOOS == "linux" {
			So(len(cfg.Raft.WAL.Options()), ShouldEqual, 1)
		} else {
			So(func() {
				cfg.Raft.WAL.Options()
			}, ShouldPanic)
		}
	})

	Convey("store config validation", t, func() {
		cfg := Config{
			MetaStore: SyncStoreConfig{
				WAL: WALConfig{
					FileSize: minMetaStoreWALFileSize - 1,
				},
			},
		}
		err := cfg.Validate()
		So(err, ShouldNotBeNil)

		cfg = Config{
			OffsetStore: AsyncStoreConfig{
				WAL: WALConfig{
					FileSize: minMetaStoreWALFileSize - 1,
				},
			},
		}
		err = cfg.Validate()
		So(err, ShouldNotBeNil)

		cfg = Config{
			Raft: RaftConfig{
				WAL: WALConfig{
					FileSize: minMetaStoreWALFileSize - 1,
				},
			},
		}
		err = cfg.Validate()
		So(err, ShouldNotBeNil)
	})
}
