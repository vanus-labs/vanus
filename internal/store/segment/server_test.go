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
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
	"testing"
	"time"

	cepb "cloudevents.io/genproto/v1"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/store/block"
	"github.com/linkall-labs/vanus/internal/store/block/file"
	"github.com/linkall-labs/vanus/internal/store/block/replica"
	"github.com/linkall-labs/vanus/internal/util"
	"github.com/linkall-labs/vanus/proto/pkg/errors"
	rpcerr "github.com/linkall-labs/vanus/proto/pkg/errors"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/linkall-labs/vanus/internal/store"
)

func TestServer_RemoveBlock(t *testing.T) {
	Convey("test RemoveBlock", t, func() {
		cfg := store.Config{}
		srv := NewServer(cfg).(*server)
		ctx := context.Background()
		Convey("test state checking", func() {
			err := srv.RemoveBlock(ctx, vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "service state error")
			So(et.Code, ShouldEqual, errors.ErrorCode_SERVICE_NOT_RUNNING)
			So(et.Message, ShouldEqual, fmt.Sprintf("the server isn't ready to work, current state: %s",
				primitive.ServerStateCreated))
		})

		srv.state = primitive.ServerStateRunning
		Convey("the replica not found", func() {
			err := srv.RemoveBlock(ctx, vanus.NewID())
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Message, ShouldEqual, "the replica not found")
		})

		ctrl := gomock.NewController(t)
		dir, _ := os.MkdirTemp("", "*")
		defer func() {
			_ = os.RemoveAll(dir)
		}()
		p := path.Join(dir, "/vanus/test/store/test")
		_ = os.MkdirAll(p, 0777)
		blk, err := file.Create(context.Background(), p, vanus.NewID(), 1024*1024*64)
		So(err, ShouldBeNil)

		Convey("the block not found", func() {
			rep := replica.NewMockReplica(ctrl)
			srv.writers.Store(blk.ID(), rep)
			srv.readers.Store(blk.ID(), blk)
			So(util.MapLen(&srv.writers), ShouldEqual, 1)
			So(util.MapLen(&srv.readers), ShouldEqual, 1)

			rep.EXPECT().Delete(ctx).Times(1)
			err := srv.RemoveBlock(ctx, blk.ID())
			So(util.MapLen(&srv.writers), ShouldEqual, 0)
			So(util.MapLen(&srv.readers), ShouldEqual, 0)
			et := err.(*errors.ErrorType)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Message, ShouldEqual, "the block not found")
		})

		Convey("test remove success", func() {
			rep := replica.NewMockReplica(ctrl)
			srv.blocks.Store(blk.ID(), blk)
			srv.writers.Store(blk.ID(), rep)
			srv.readers.Store(blk.ID(), blk)
			So(util.MapLen(&srv.blocks), ShouldEqual, 1)
			So(util.MapLen(&srv.writers), ShouldEqual, 1)
			So(util.MapLen(&srv.readers), ShouldEqual, 1)

			rep.EXPECT().Delete(ctx).Times(1)
			err := srv.RemoveBlock(ctx, blk.ID())
			So(util.MapLen(&srv.blocks), ShouldEqual, 0)
			So(util.MapLen(&srv.writers), ShouldEqual, 0)
			So(util.MapLen(&srv.readers), ShouldEqual, 0)
			So(err, ShouldBeNil)
			_, err = os.Open(blk.Path())
			So(os.IsNotExist(err), ShouldBeTrue)
		})
	})
}

func TestServer_ReadFromBlock(t *testing.T) {
	Convey("Test segment read block whether exist offset", t, func() {
		volumeDir, _ := os.MkdirTemp("", "volume-*")
		defer func() {
			_ = os.RemoveAll(volumeDir)
		}()
		Convey("no long-polling", func() {
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
			_ = srv.Initialize(ctx)

			blockID := vanus.NewIDFromUint64(1)
			err := srv.CreateBlock(ctx, blockID, 1024*1024)
			So(err, ShouldBeNil)

			replicas := map[vanus.ID]string{
				blockID: srv.localAddress,
			}
			_ = srv.ActivateSegment(ctx, 1, 1, replicas)

			time.Sleep(3 * time.Second) // make sure that there is a leader elected in Raft.

			events := []*cepb.CloudEvent{{
				Id: "123",
				Data: &cepb.CloudEvent_TextData{
					TextData: "hello world!",
				},
			}}
			err = srv.AppendToBlock(ctx, blockID, events)
			So(err, ShouldBeNil)
			_ = srv.AppendToBlock(ctx, blockID, events)
			_ = srv.AppendToBlock(ctx, blockID, events)

			time.Sleep(3 * time.Second)

			pbEvents, err := srv.ReadFromBlock(ctx, blockID, 0, 3)
			So(err, ShouldBeNil)
			for i, pbEvent := range pbEvents {
				So(pbEvent.Attributes[segpb.XVanusBlockOffset].Attr.(*cepb.CloudEventAttributeValue_CeInteger).CeInteger, ShouldEqual, i)
			}
		})

		Convey("long-polling without timeout", func() {
			srv := &server{}
			srv.state = primitive.ServerStateRunning
			ctrl := gomock.NewController(t)
			pmMock := NewMockpollingManager(ctrl)
			srv.pm = pmMock
			reader := block.NewMockReader(ctrl)
			blkID := vanus.NewID()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := srv.ReadFromBlock(ctx, blkID, 3, 5)
			So(err, ShouldNotBeNil)
			So(err.(*rpcerr.ErrorType).Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)

			srv.readers.Store(blkID, reader)
			newMessageArrived := false
			reader.EXPECT().Read(ctx, 3, 5).Times(2).DoAndReturn(func(ctx context.Context, off, num int) ([]block.Entry, error) {
				if !newMessageArrived {
					return nil, block.ErrOffsetOnEnd
				}
				ce := &cepb.CloudEvent{
					Id: "123",
					Data: &cepb.CloudEvent_TextData{
						TextData: "hello world!",
					},
				}
				data, _ := proto.Marshal(ce)
				var events = []block.Entry{{
					Offset:  3,
					Index:   3,
					Payload: data,
				}}
				return events, nil
			})
			ch := make(chan struct{})
			pmMock.EXPECT().Add(gomock.Any(), blkID).Times(1).Return(ch)
			start := time.Now()
			go func() {
				time.Sleep(time.Second)
				newMessageArrived = true
				close(ch)
			}()
			events, _ := srv.ReadFromBlock(ctx, blkID, 3, 5)
			So(events, ShouldHaveLength, 1)
			So(events[0].Id, ShouldEqual, "123")
			So(time.Now().Sub(start), ShouldBeGreaterThan, 800*time.Millisecond)
		})

		Convey("long-polling with timeout", func() {
			srv := &server{}
			srv.state = primitive.ServerStateRunning
			ctrl := gomock.NewController(t)
			pmMock := NewMockpollingManager(ctrl)
			srv.pm = pmMock
			reader := block.NewMockReader(ctrl)
			blkID := vanus.NewID()

			ctx, cancel := context.WithCancel(context.Background())
			_, err := srv.ReadFromBlock(ctx, blkID, 3, 5)
			So(err, ShouldNotBeNil)
			So(err.(*rpcerr.ErrorType).Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)

			srv.readers.Store(blkID, reader)
			reader.EXPECT().Read(ctx, 3, 5).Times(1).Return(nil, block.ErrOffsetOnEnd)
			ch := make(chan struct{})
			pmMock.EXPECT().Add(gomock.Any(), blkID).Times(1).Return(ch)
			defer close(ch)
			start := time.Now()
			go func() {
				time.Sleep(time.Second)
				cancel()
			}()
			_, err = srv.ReadFromBlock(ctx, blkID, 3, 5)
			So(err, ShouldEqual, block.ErrOffsetOnEnd)
			So(time.Now().Sub(start), ShouldBeGreaterThan, 800*time.Millisecond)
		})
	})
}
