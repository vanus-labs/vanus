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
	"testing"

	// third-party libraries.
	. "github.com/smartystreets/goconvey/convey"

	// this project.
	"github.com/vanus-labs/vanus/internal/store/config"
)

const fileSize = 4*1024*1024 - 1

func TestConfig(t *testing.T) {
	Convey("store config validation", t, func() {
		cfg := Config{
			MetaStore: config.SyncStore{
				WAL: config.WAL{
					FileSize: fileSize,
				},
			},
		}
		err := cfg.Validate()
		So(err, ShouldNotBeNil)

		cfg = Config{
			OffsetStore: config.AsyncStore{
				WAL: config.WAL{
					FileSize: fileSize,
				},
			},
		}
		err = cfg.Validate()
		So(err, ShouldNotBeNil)

		cfg = Config{
			Raft: config.Raft{
				WAL: config.WAL{
					FileSize: fileSize,
				},
			},
		}
		err = cfg.Validate()
		So(err, ShouldNotBeNil)
	})
}
