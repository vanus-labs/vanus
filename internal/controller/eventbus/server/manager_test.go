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

package server

import (
	stdCtx "context"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/errors"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/pkg/util"
	segpb "github.com/linkall-labs/vanus/proto/pkg/segment"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

func TestSegmentServerManager_AddAndRemoveServer(t *testing.T) {
	Convey("test add and remove server", t, func() {
		mgr := NewServerManager()
		ss1, err := NewSegmentServer("127.0.0.1:10001")
		So(err, ShouldBeNil)
		err = mgr.AddServer(stdCtx.Background(), ss1)
		So(err, ShouldBeNil)
		ssm := mgr.(*segmentServerManager)

		ss2, err := NewSegmentServer("127.0.0.1:10002")
		So(err, ShouldBeNil)
		err = mgr.AddServer(stdCtx.Background(), ss2)
		So(err, ShouldBeNil)

		ss3, err := NewSegmentServer("127.0.0.1:10003")
		So(err, ShouldBeNil)
		err = mgr.AddServer(stdCtx.Background(), ss3)
		So(err, ShouldBeNil)
		So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 3)
		So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 3)
		_, exist := ssm.segmentServerMapByIP.Load(ss1.Address())
		So(exist, ShouldBeTrue)
		_, exist = ssm.segmentServerMapByID.Load(ss1.ID().Key())
		So(exist, ShouldBeTrue)
		_, exist = ssm.segmentServerMapByIP.Load(ss2.Address())
		So(exist, ShouldBeTrue)
		_, exist = ssm.segmentServerMapByID.Load(ss2.ID().Key())
		So(exist, ShouldBeTrue)
		_, exist = ssm.segmentServerMapByIP.Load(ss3.Address())
		So(exist, ShouldBeTrue)
		_, exist = ssm.segmentServerMapByID.Load(ss3.ID().Key())
		So(exist, ShouldBeTrue)

		Convey("test remove server", func() {
			err = mgr.RemoveServer(stdCtx.Background(), ss1)
			So(err, ShouldBeNil)
			_, exist = ssm.segmentServerMapByIP.Load(ss1.Address())
			So(exist, ShouldBeFalse)
			_, exist = ssm.segmentServerMapByID.Load(ss1.ID().Key())
			So(exist, ShouldBeFalse)
			So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 2)
			So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 2)
		})

		Convey("test add a invalid server", func() {
			err = mgr.AddServer(stdCtx.Background(), nil)
			So(err, ShouldBeNil)
			err = mgr.AddServer(stdCtx.Background(), ss1)
			So(err, ShouldBeNil)

			ss4, err := NewSegmentServer("127.0.0.1:10003")
			So(err, ShouldBeNil)

			err = mgr.AddServer(stdCtx.Background(), ss4)
			So(err, ShouldEqual, errors.ErrSegmentServerHasBeenAdded)
			So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 3)
			So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 3)
		})

		Convey("test get by ip and id", func() {
			So(mgr.GetServerByAddress(ss1.Address()), ShouldEqual, ss1)
			So(mgr.GetServerByServerID(ss1.ID()), ShouldEqual, ss1)
			So(mgr.GetServerByAddress(ss2.Address()), ShouldEqual, ss2)
			So(mgr.GetServerByServerID(ss2.ID()), ShouldEqual, ss2)
			So(mgr.GetServerByAddress(ss3.Address()), ShouldEqual, ss3)
			So(mgr.GetServerByServerID(ss3.ID()), ShouldEqual, ss3)
		})
	})
}

func TestSegmentServerManager_Run(t *testing.T) {
	Convey("test Manager run and stop", t, func() {
		mgr := NewServerManager()
		ss1, err := NewSegmentServer("127.0.0.1:10001")
		So(err, ShouldBeNil)
		err = mgr.AddServer(stdCtx.Background(), ss1)
		So(err, ShouldBeNil)
		ssm := mgr.(*segmentServerManager)

		ss2, err := NewSegmentServer("127.0.0.1:10002")
		So(err, ShouldBeNil)
		err = mgr.AddServer(stdCtx.Background(), ss2)
		So(err, ShouldBeNil)
		So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 2)
		So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 2)

		ssm.ticker = time.NewTicker(50 * time.Millisecond)
		ctrl := gomock.NewController(t)
		_ss1 := ss1.(*segmentServer)
		_ss2 := ss2.(*segmentServer)
		mockSSCli1 := segpb.NewMockSegmentServerClient(ctrl)
		mockSSCli2 := segpb.NewMockSegmentServerClient(ctrl)
		_ss1.client = mockSSCli1
		_ss2.client = mockSSCli2

		mutex = sync.Mutex{}
		status1 := "running"
		status2 := "running"
		f1 := func(ctx stdCtx.Context, empty *empty.Empty, opts ...grpc.CallOption) (*segpb.StatusResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			return &segpb.StatusResponse{
				Status: status1,
			}, nil
		}
		f2 := func(ctx stdCtx.Context, empty *empty.Empty, opts ...grpc.CallOption) (*segpb.StatusResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			return &segpb.StatusResponse{
				Status: status2,
			}, nil
		}
		mockSSCli1.EXPECT().Status(stdCtx.Background(), gomock.Any()).AnyTimes().DoAndReturn(f1)
		mockSSCli2.EXPECT().Status(stdCtx.Background(), gomock.Any()).AnyTimes().DoAndReturn(f2)
		err = ssm.Run(stdCtx.Background())
		So(err, ShouldBeNil)
		time.Sleep(200 * time.Millisecond)
		So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 2)
		So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 2)

		mutex.Lock()
		status1 = "stopped"
		mutex.Unlock()
		time.Sleep(200 * time.Millisecond)
		So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 1)
		So(util.MapLen(&ssm.segmentServerMapByIP), ShouldEqual, 1)

		mgr.Stop(stdCtx.Background())
		mutex.Lock()
		status2 = "stopped"
		mutex.Unlock()
		time.Sleep(200 * time.Millisecond)
		So(util.MapLen(&ssm.segmentServerMapByID), ShouldEqual, 1)
	})
}

func TestSegmentServer(t *testing.T) {
	Convey("test Manager run and stop", t, func() {
		ss1, err := NewSegmentServer("127.0.0.1:10001")
		So(err, ShouldBeNil)

		ctrl := gomock.NewController(t)
		_ss1 := ss1.(*segmentServer)
		mockSSCli1 := segpb.NewMockSegmentServerClient(ctrl)
		_ss1.client = mockSSCli1

		So(ss1.Address(), ShouldEqual, "127.0.0.1:10001")
		So(ss1.GetClient(), ShouldEqual, mockSSCli1)

		status1 := "running"
		f1 := func(ctx stdCtx.Context, empty *empty.Empty, opts ...grpc.CallOption) (*segpb.StatusResponse, error) {
			return &segpb.StatusResponse{
				Status: status1,
			}, nil
		}
		ctx := stdCtx.Background()
		mockSSCli1.EXPECT().Status(stdCtx.Background(), gomock.Any()).AnyTimes().DoAndReturn(f1)
		So(ss1.IsActive(ctx), ShouldBeTrue)
		status1 = "stopped"
		So(ss1.IsActive(ctx), ShouldBeFalse)

		mockSSCli1.EXPECT().Start(ctx, gomock.Any(), gomock.Any()).Times(1).DoAndReturn(
			func(ctx stdCtx.Context, req *segpb.StartSegmentServerRequest,
				opts ...grpc.CallOption) (*segpb.StartSegmentServerResponse, error) {
				So(req.ServerId, ShouldEqual, ss1.ID())
				return &segpb.StartSegmentServerResponse{}, nil
			})
		So(ss1.RemoteStart(ctx), ShouldBeNil)

		mockSSCli1.EXPECT().Stop(ctx, gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)
		ss1.RemoteStop(ctx)

		So(ss1.Close(), ShouldBeNil)

		ss2, err := NewSegmentServerWithID(vanus.NewIDFromUint64(1024), "127.0.0.1:10001")
		So(err, ShouldBeNil)
		So(ss2.ID(), ShouldEqual, vanus.NewIDFromUint64(1024))
	})
}
