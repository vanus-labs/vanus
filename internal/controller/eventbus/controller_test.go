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

	"github.com/linkall-labs/vanus/internal/controller/eventbus/eventlog"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	"github.com/linkall-labs/vanus/proto/pkg/errors"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestController_CreateEventBus(t *testing.T) {
	Convey("test create eventbus", t, func() {
		cfg := Config{}
		cfg.Topology = map[string]string{"1": "a", "2": "b"}
		ctrl := NewController(cfg, nil)
		mockCtrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(mockCtrl)
		ctrl.kvStore = kvCli
		elMgr := eventlog.NewMockManager(mockCtrl)
		ctrl.eventLogMgr = elMgr
		ctx := stdCtx.Background()

		Convey("test create a eventbus two times", func() {
			kvCli.EXPECT().Exists(ctx, metadata.GetEventbusMetadataKey("test-1")).Times(1).Return(false, nil)
			kvCli.EXPECT().Set(ctx, metadata.GetEventbusMetadataKey("test-1"), gomock.Any()).
				Times(1).Return(nil)
			el := &metadata.Eventlog{
				ID: vanus.NewID(),
			}
			elMgr.EXPECT().AcquireEventLog(ctx, gomock.Any()).Times(1).DoAndReturn(func(ctx stdCtx.Context,
				eventbusID vanus.ID) (*metadata.Eventlog, error) {
				el.ID = eventbusID
				el.SegmentNumber = 2
				return el, nil
			})

			res, err := ctrl.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name:      "test-1",
				LogNumber: 0,
			})
			So(err, ShouldBeNil)
			So(res.Name, ShouldEqual, "test-1")
			So(res.Id, ShouldEqual, el.EventbusID.Uint64())
			So(res.Logs, ShouldHaveLength, 1)
			So(res.LogNumber, ShouldEqual, 1)
			So(res.Logs[0].EventBusName, ShouldEqual, "test-1")
			sort.Strings(res.Logs[0].ServerAddress)
			So(res.Logs[0].ServerAddress, ShouldResemble, []string{"a", "b"})
			So(res.Logs[0].CurrentSegmentNumbers, ShouldEqual, 2)

			_, exist := ctrl.eventBusMap["test-1"]
			So(exist, ShouldBeTrue)
		})

		Convey("test create a eventbus but exist", func() {
			kvCli.EXPECT().Exists(ctx, metadata.GetEventbusMetadataKey("test-1")).Times(1).Return(true, nil)

			res, err := ctrl.CreateEventBus(ctx, &ctrlpb.CreateEventBusRequest{
				Name:      "test-1",
				LogNumber: 0,
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

func TestController_DeleteEventBus(t *testing.T) {
	Convey("test delete a eventbus ", t, func() {
		cfg := Config{}
		cfg.Topology = map[string]string{"1": "a", "2": "b"}
		ctrl := NewController(cfg, nil)
		mockCtrl := gomock.NewController(t)
		kvCli := kv.NewMockClient(mockCtrl)
		ctrl.kvStore = kvCli
		elMgr := eventlog.NewMockManager(mockCtrl)
		ctrl.eventLogMgr = elMgr
		ctx := stdCtx.Background()

		Convey("deleting a doesn't exist eventbus", func() {
			res, err := ctrl.DeleteEventBus(ctx, &metapb.EventBus{Name: "test-1"})
			So(err, ShouldNotBeNil)
			So(res, ShouldBeNil)
			et, ok := err.(*errors.ErrorType)
			So(ok, ShouldBeTrue)
			So(et.Code, ShouldEqual, errors.ErrorCode_RESOURCE_NOT_FOUND)
			So(et.Description, ShouldEqual, "resource not found")
			So(et.Message, ShouldEqual, "the eventbus doesn't exist")
		})

		md := &metadata.Eventbus{
			ID:        vanus.NewID(),
			Name:      "test-1",
			LogNumber: 2,
			EventLogs: []*metadata.Eventlog{
				{
					ID: vanus.NewID(),
				},
				{
					ID: vanus.NewID(),
				},
			},
		}

		Convey("deleting an existed eventbus, but kv error", func() {
			kvCli.EXPECT().Delete(ctx, metadata.GetEventbusMetadataKey("test-1")).Times(1).
				Return(fmt.Errorf("test"))

			ctrl.eventBusMap["test-1"] = md
			res, err := ctrl.DeleteEventBus(stdCtx.Background(), &metapb.EventBus{Name: "test-1"})
			So(err, ShouldNotBeNil)
			So(res, ShouldBeNil)
			et, ok := err.(*errors.ErrorType)
			So(ok, ShouldBeTrue)
			So(et.Code, ShouldEqual, errors.ErrorCode_INTERNAL)
			So(et.Description, ShouldEqual, "internal error")
			So(et.Message, ShouldEqual, "delete eventbus metadata in kv failed")
		})

		Convey("deleting an existed eventbus success", func() {
			kvCli.EXPECT().Delete(ctx, metadata.GetEventbusMetadataKey("test-1")).Times(1).
				Return(nil)

			elMgr.EXPECT().DeleteEventlog(ctx, md.EventLogs[0].ID).Times(1)
			elMgr.EXPECT().DeleteEventlog(ctx, md.EventLogs[1].ID).Times(1)

			ctrl.eventBusMap["test-1"] = md
			_, err := ctrl.DeleteEventBus(stdCtx.Background(), &metapb.EventBus{Name: "test-1"})
			So(err, ShouldBeNil)

			_, exist := ctrl.eventBusMap["test-1"]
			So(exist, ShouldBeFalse)
		})
	})
}
