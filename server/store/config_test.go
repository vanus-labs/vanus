// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package store

import (
	// standard libraries.
	"os"
	"runtime"
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/config"
)

func TestConfig(t *testing.T) {
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
  dir: /vanus/volume/store-1
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
		So(cfg.Volume.Dir, ShouldEqual, "/vanus/volume/store-1")
		So(cfg.Volume.Capacity, ShouldEqual, 536870912)

		So(cfg.MetaStore.WAL.FileSize, ShouldEqual, 4194304)
		So(cfg.MetaStore.WAL.IO.Engine, ShouldEqual, config.Psync)
		So(len(cfg.MetaStore.WAL.Options()), ShouldEqual, 2)

		So(cfg.OffsetStore.WAL.IO.Engine, ShouldEqual, "")
		So(len(cfg.OffsetStore.WAL.Options()), ShouldEqual, 0)

		So(cfg.Raft.WAL.IO.Engine, ShouldEqual, config.Uring)
		if runtime.GOOS == "linux" {
			So(len(cfg.Raft.WAL.Options()), ShouldEqual, 1)
		} else {
			So(func() {
				cfg.Raft.WAL.Options()
			}, ShouldPanic)
		}
	})
}
