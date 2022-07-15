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

package wal

import (
	// standard libraries.
	"testing"
	"time"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/linkall-labs/vanus/internal/store/io"
)

func TestConfig(t *testing.T) {
	Convey("wal config", t, func() {
		engine := io.NewEngine()
		cfg := makeConfig(
			FromPosition(1024),
			WithBlockSize(1024),
			WithFileSize(4*1024*1024),
			WithFlushTimeout(5*time.Millisecond),
			WithAppendBufferSize(1),
			WithCallbackBufferSize(1),
			WithFlushBufferSize(1),
			WithWakeupBufferSize(1),
			WithIOEngine(engine),
		)

		So(cfg.pos, ShouldEqual, 1024)
		So(cfg.blockSize, ShouldEqual, 1024)
		So(cfg.fileSize, ShouldEqual, 4*1024*1024)
		So(cfg.flushTimeout, ShouldEqual, 5*time.Millisecond)
		So(cfg.appendBufferSize, ShouldEqual, 1)
		So(cfg.callbackBufferSize, ShouldEqual, 1)
		So(cfg.flushBufferSize, ShouldEqual, 1)
		So(cfg.wakeupBufferSize, ShouldEqual, 1)
		So(cfg.engine, ShouldEqual, engine)
	})
}
