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

package eventbus

import (
	stdCtx "context"
	"fmt"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/vanus-labs/vanus/internal/controller/eventbus/eventlog"
	"github.com/vanus-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/kv"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
	"github.com/vanus-labs/vanus/pkg/cluster"
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestController_CreateEventbus(t *testing.T) {
	Convey("test create eventbus", t, func() {
		cfg := Config{}
		cfg.Topology = map[string]string{"1": "a", "2": "b"}
		ctrl := NewController(cfg, nil)
		mockCtrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(mockCtrl)
		ctrl.kvStore = kvCli
		elMgr := eventlog.NewMockManager(mockCtrl)
		ctrl.eventlogMgr = elMgr
		ctx := stdCtx.Background()

		mockMember := member.NewMockMember(mockCtrl)
		ctrl.member = mockMember
		mockMember.EXPECT().IsLeader().AnyTimes().Return(true)
		mockMember.EXPECT().IsReady().AnyTimes().Return(true)
		mockMember.EXPECT().GetLeaderAddr().AnyTimes().Return("test")

		defaultNS := vanus.NewTestID()
		systemNS := vanus.NewTestID()
		mockCluster := cluster.NewMockCluster(mockCtrl)
		ctrl.clusterCli = mockCluster
		mockNS := cluster.NewMockNamespaceService(mockCtrl)
		mockCluster.EXPECT().NamespaceService().AnyTimes().Return(mockNS)

		Convey("test create a eventbus two times", func() {
			mockNS.EXPECT().GetNamespace(ctx, defaultNS.Uint64()).Times(1).Return(&metapb.Namespace{
				Id:   defaultNS.Uint64(),
				Name: "default",
			}, nil)
			mockNS.EXPECT().GetSystemNamespace(ctx).Times(1).Return(&metapb.Namespace{
				Id:   systemNS.Uint64(),
				Name: "vanus-system",
			}, nil)

			kvCli.EXPECT().Exists(ctx, gomock.Any()).Times(1).Return(false, nil)
			kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).
				Times(1).Return(nil)
			kvCli.EXPECT().Exists(ctx, gomock.Any()).Times(1).Return(false, nil)
			kvCli.EXPECT().Set(ctx, gomock.Any(), gomock.Any()).
				Times(1).Return(nil)
			el := &metadata.Eventlog{
				ID: vanus.NewTestID(),
			}
			elMgr.EXPECT().AcquireEventlog(ctx, gomock.Any(), gomock.Any()).Times(2).DoAndReturn(func(ctx stdCtx.Context,
				eventbusID vanus.ID, ebName string,
			) (*metadata.Eventlog, error) {
				el.ID = eventbusID
				el.EventbusName = ebName
				el.SegmentNumber = 2
				return el, nil
			})

			vanus.InitFakeSnowflake()
			res, err := ctrl.CreateEventbus(ctx, &ctrlpb.CreateEventbusRequest{
				Name:        "test-1",
				LogNumber:   0,
				NamespaceId: defaultNS.Uint64(),
			})
			So(err, ShouldBeNil)
			So(res.Name, ShouldEqual, "test-1")
			So(res.Id, ShouldNotEqual, 0)
			So(res.Logs, ShouldHaveLength, 1)
			So(res.LogNumber, ShouldEqual, 1)
			So(res.Logs[0].EventbusId, ShouldEqual, res.Id)
			sort.Strings(res.Logs[0].ServerAddress)
			So(res.Logs[0].ServerAddress, ShouldResemble, []string{"a", "b"})
			So(res.Logs[0].CurrentSegmentNumbers, ShouldEqual, 2)

			_, exist := ctrl.eventbusMap[vanus.NewIDFromUint64(res.Id)]
			So(exist, ShouldBeTrue)
		})

		Convey("test create a eventbus but exist", func() {
			mockNS.EXPECT().GetNamespace(ctx, defaultNS.Uint64()).Times(1).Return(&metapb.Namespace{
				Id:   defaultNS.Uint64(),
				Name: "default",
			}, nil)
			kvCli.EXPECT().Exists(ctx, gomock.Any()).Times(1).Return(true, nil)

			res, err := ctrl.CreateEventbus(ctx, &ctrlpb.CreateEventbusRequest{
				Name:        "test-1",
				LogNumber:   0,
				NamespaceId: defaultNS.Uint64(),
			})
			So(err, ShouldNotBeNil)
			So(res, ShouldBeNil)
			et, ok := err.(*errors.ErrorType)
			So(ok, ShouldBeTrue)
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_EXIST)
			So(et.Description, ShouldEqual, "resource already exist")
			So(et.Message, ShouldEqual, "the eventbus already exist")
		})
	})
}

func TestController_DeleteEventbus(t *testing.T) {
	Convey("test delete a eventbus ", t, func() {
		cfg := Config{}
		cfg.Topology = map[string]string{"1": "a", "2": "b"}
		ctrl := NewController(cfg, nil)
		mockCtrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(mockCtrl)
		ctrl.kvStore = kvCli
		elMgr := eventlog.NewMockManager(mockCtrl)
		ctrl.eventlogMgr = elMgr
		ctx := stdCtx.Background()

		Convey("deleting a doesn't exist eventbus", func() {
			res, err := ctrl.DeleteEventbus(ctx, &wrapperspb.UInt64Value{Value: vanus.NewTestID().Uint64()})
			So(err, ShouldNotBeNil)
			So(res, ShouldBeNil)
			et, ok := err.(*errors.ErrorType)
			So(ok, ShouldBeTrue)
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Message, ShouldEqual, "the eventbus doesn't exist")
		})

		md := &metadata.Eventbus{
			ID:        vanus.NewTestID(),
			Name:      "test-1",
			LogNumber: 2,
			Eventlogs: []*metadata.Eventlog{
				{
					ID: vanus.NewTestID(),
				},
				{
					ID: vanus.NewTestID(),
				},
			},
		}

		Convey("deleting an existed eventbus, but kv error", func() {
			kvCli.EXPECT().Delete(ctx, gomock.Any()).Times(1).
				Return(fmt.Errorf("test"))

			ctrl.eventbusMap[md.ID] = md
			res, err := ctrl.DeleteEventbus(stdCtx.Background(), &wrapperspb.UInt64Value{Value: md.ID.Uint64()})
			So(err, ShouldNotBeNil)
			So(res, ShouldBeNil)
			et, ok := err.(*errors.ErrorType)
			So(ok, ShouldBeTrue)
			So(et.Code, ShouldEqual, errors.ErrorCode_INTERNAL)
			So(et.Description, ShouldEqual, "internal error")
			So(et.Message, ShouldEqual, "delete eventbus metadata in kv failed")
		})

		Convey("deleting an existed eventbus success", func() {
			kvCli.EXPECT().Delete(ctx, gomock.Any()).Times(1).
				Return(nil)

			elMgr.EXPECT().DeleteEventlog(ctx, md.Eventlogs[0].ID).Times(1)
			elMgr.EXPECT().DeleteEventlog(ctx, md.Eventlogs[1].ID).Times(1)

			ctrl.eventbusMap[md.ID] = md
			_, err := ctrl.DeleteEventbus(stdCtx.Background(), &wrapperspb.UInt64Value{Value: md.ID.Uint64()})
			So(err, ShouldBeNil)

			_, exist := ctrl.eventbusMap[md.ID]
			So(exist, ShouldBeFalse)
		})
	})
}
