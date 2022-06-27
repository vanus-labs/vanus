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

package gateway

import (
	stdCtx "context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

func Test_ControllerProxy(t *testing.T) {
	Convey("test grpc reverse proxy", t, func() {
		listen1, err := net.Listen("tcp", fmt.Sprintf(":%d", 20001))
		So(err, ShouldBeNil)
		ctrl := gomock.NewController(t)
		pingSvc1 := ctrlpb.NewMockPingServerServer(ctrl)
		ebSvc1 := ctrlpb.NewMockEventBusControllerServer(ctrl)
		srv1 := grpc.NewServer()
		ctrlpb.RegisterPingServerServer(srv1, pingSvc1)
		ctrlpb.RegisterEventBusControllerServer(srv1, ebSvc1)
		defer srv1.GracefulStop()
		go func() {
			_ = srv1.Serve(listen1)
		}()

		listen2, err := net.Listen("tcp", fmt.Sprintf(":%d", 20002))
		So(err, ShouldBeNil)
		pingSvc2 := ctrlpb.NewMockPingServerServer(ctrl)
		ebSvc2 := ctrlpb.NewMockEventBusControllerServer(ctrl)
		srv2 := grpc.NewServer()
		ctrlpb.RegisterPingServerServer(srv2, pingSvc2)
		ctrlpb.RegisterEventBusControllerServer(srv2, ebSvc2)
		defer srv2.GracefulStop()
		go func() {
			_ = srv2.Serve(listen2)
		}()

		listen3, err := net.Listen("tcp", fmt.Sprintf(":%d", 20003))
		So(err, ShouldBeNil)
		pingSvc3 := ctrlpb.NewMockPingServerServer(ctrl)
		ebSvc3 := ctrlpb.NewMockEventBusControllerServer(ctrl)
		srv3 := grpc.NewServer()
		ctrlpb.RegisterPingServerServer(srv3, pingSvc3)
		ctrlpb.RegisterEventBusControllerServer(srv3, ebSvc3)
		defer srv3.GracefulStop()
		go func() {
			_ = srv3.Serve(listen3)
		}()

		ctx, cancel := stdCtx.WithCancel(stdCtx.Background())
		cp := newCtrlProxy(20000, map[string]string{}, []string{"127.0.0.1:20001",
			"127.0.0.1:20002", "127.0.0.1:20003"})
		cp.ticker = time.NewTicker(10 * time.Millisecond)
		err = cp.start(ctx)
		So(err, ShouldBeNil)
		var opts []grpc.DialOption
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		conn, err := grpc.Dial("127.0.0.1:20000", opts...)
		So(err, ShouldBeNil)

		pingRes := &ctrlpb.PingResponse{
			LeaderAddr:  "127.0.0.1:20001",
			GatewayAddr: "127.0.0.1:12345",
		}
		mutex := sync.Mutex{}
		pingSvc1.EXPECT().Ping(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			in *emptypb.Empty) (*ctrlpb.PingResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			return pingRes, nil
		})
		pingSvc2.EXPECT().Ping(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			in *emptypb.Empty) (*ctrlpb.PingResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			return pingRes, nil
		})
		pingSvc3.EXPECT().Ping(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			in *emptypb.Empty) (*ctrlpb.PingResponse, error) {
			mutex.Lock()
			defer mutex.Unlock()
			return pingRes, nil
		})

		Convey("test allow methods", func() {
			pingCli := ctrlpb.NewPingServerClient(conn)
			res, err := pingCli.Ping(stdCtx.Background(), &empty.Empty{})
			So(res, ShouldBeNil)
			So(err.Error(), ShouldContainSubstring, "No leader founded")
			time.Sleep(100 * time.Millisecond)
			_, err = pingCli.Ping(stdCtx.Background(), &empty.Empty{})
			So(err.Error(), ShouldContainSubstring, "Unknown method")
		})

		Convey("test ping", func() {
			cp.allowProxyMethod["/linkall.vanus.controller.PingServer/Ping"] = "ALLOW"
			pingCli := ctrlpb.NewPingServerClient(conn)
			time.Sleep(100 * time.Millisecond)
			res, err := pingCli.Ping(stdCtx.Background(), &empty.Empty{})
			So(err, ShouldBeNil)
			So(res.LeaderAddr, ShouldEqual, "127.0.0.1:20001")
			So(res.GatewayAddr, ShouldEqual, "127.0.0.1:12345")

			mutex.Lock()
			pingRes.LeaderAddr = "127.0.0.1:20003"
			mutex.Unlock()
			time.Sleep(100 * time.Millisecond)
			res, err = pingCli.Ping(stdCtx.Background(), &empty.Empty{})
			So(err, ShouldBeNil)
			So(res.LeaderAddr, ShouldEqual, "127.0.0.1:20003")
			So(res.GatewayAddr, ShouldEqual, "127.0.0.1:12345")
		})

		Convey("test list eventbus", func() {
			cp.allowProxyMethod["/linkall.vanus.controller.PingServer/Ping"] = "ALLOW"
			cp.allowProxyMethod["/linkall.vanus.controller.EventBusController/ListEventBus"] = "ALLOW"
			ebCli := ctrlpb.NewEventBusControllerClient(conn)
			time.Sleep(100 * time.Millisecond)

			ebSvc1.EXPECT().ListEventBus(gomock.Any(), gomock.Any()).Times(1).Return(&ctrlpb.ListEventbusResponse{
				Eventbus: []*metapb.EventBus{
					{
						Name:      "battle1",
						LogNumber: 1,
						Id:        1,
					},
					{
						Name:      "battle2",
						LogNumber: 2,
						Id:        2,
					},
					{
						Name:      "battle3",
						LogNumber: 4,
						Id:        3,
					},
					{
						Name:      "battle4",
						LogNumber: 4,
						Id:        4,
					},
				},
			}, nil)
			res, err := ebCli.ListEventBus(ctx, &empty.Empty{})
			So(err, ShouldBeNil)
			So(res.Eventbus, ShouldHaveLength, 4)
			So(res.Eventbus[0].Name, ShouldEqual, "battle1")
			So(res.Eventbus[0].Id, ShouldEqual, 1)
			So(res.Eventbus[1].Name, ShouldEqual, "battle2")
			So(res.Eventbus[1].Id, ShouldEqual, 2)
			So(res.Eventbus[2].Name, ShouldEqual, "battle3")
			So(res.Eventbus[2].Id, ShouldEqual, 3)
			So(res.Eventbus[3].Name, ShouldEqual, "battle4")
			So(res.Eventbus[3].Id, ShouldEqual, 4)
		})

		cancel()
		time.Sleep(100 * time.Millisecond)
	})
}
