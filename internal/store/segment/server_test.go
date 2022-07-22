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

package segment

import (
	"context"
	"os"
	"testing"
	"time"

	cepb "cloudevents.io/genproto/v1"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSegmentReadBlock(t *testing.T) {
	Convey("Test segment read block whether exist offset", t, func() {
		volumeDir, _ := os.MkdirTemp("", "volume-*")
		defer os.RemoveAll(volumeDir)

		cfg := store.Config{
			IP:   "127.0.0.1",
			Port: 2148,
			Volume: store.VolumeInfo{
				ID:       123456,
				Dir:      volumeDir,
				Capacity: 137975824384,
			},
			MetaStore: store.SyncStoreConfig{
				WAL: store.WALConfig{
					FileSize: 1024 * 1024,
					IO: store.IOConfig{
						Engine: "psync",
					},
				},
			},
			OffsetStore: store.AsyncStoreConfig{
				WAL: store.WALConfig{
					FileSize: 1024 * 1024,
					IO: store.IOConfig{
						Engine: "psync",
					},
				},
			},
			Raft: store.RaftConfig{
				WAL: store.WALConfig{
					FileSize: 1024 * 1024,
					IO: store.IOConfig{
						Engine: "psync",
					},
				},
			},
		}

		srv := NewServer(cfg).(*server)
		ctx := context.Background()

		srv.isDebugMode = true
		srv.Initialize(ctx)

		blockID := vanus.NewIDFromUint64(1)
		err := srv.CreateBlock(ctx, blockID, 1024*1024)
		So(err, ShouldBeNil)

		replicas := map[vanus.ID]string{
			blockID: srv.localAddress,
		}
		srv.ActivateSegment(ctx, 1, 1, replicas)

		time.Sleep(3 * time.Second) // make sure that there is a leader elected in Raft.

		events := []*cepb.CloudEvent{{
			Id: "123",
			Data: &cepb.CloudEvent_TextData{
				TextData: "hello world!",
			},
		}}
		err = srv.AppendToBlock(ctx, blockID, events)
		So(err, ShouldBeNil)
		srv.AppendToBlock(ctx, blockID, events)
		srv.AppendToBlock(ctx, blockID, events)

		time.Sleep(3 * time.Second)

		pbEvents, err := srv.ReadFromBlock(ctx, blockID, 0, 3)
		So(err, ShouldBeNil)
		for i, pbEvent := range pbEvents {
			So(pbEvent.Attributes["xvanusblockoffset"].Attr.(*cepb.CloudEventAttributeValue_CeInteger).CeInteger, ShouldEqual, i)
		}
	})
}
