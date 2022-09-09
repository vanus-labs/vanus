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

package trigger

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/trigger"
	"github.com/linkall-labs/vanus/proto/pkg/controller"
	. "github.com/smartystreets/goconvey/convey"
)

func testNewTrigger(t trigger.Trigger) newTrigger {
	return func(sub *primitive.Subscription, opts ...trigger.Option) trigger.Trigger {
		return t
	}
}
func TestAddSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("add subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tg := trigger.NewMockTrigger(ctrl)
		m := NewWorker(Config{ControllerAddr: []string{"test"}}).(*worker)
		m.newTrigger = testNewTrigger(tg)
		Convey("add subscription", func() {
			id := vanus.NewID()
			tg.EXPECT().Init(gomock.Any()).Return(nil)
			tg.EXPECT().Start(gomock.Any()).Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			v, exist := m.getTrigger(id)
			So(exist, ShouldBeTrue)
			So(v, ShouldNotBeNil)
			Convey("update subscription", func() {
				tg.EXPECT().Change(gomock.Any(), gomock.Any()).Return(nil)
				err = m.AddSubscription(ctx, &primitive.Subscription{
					ID:   id,
					Sink: "http://localhost:8080",
				})
				So(err, ShouldBeNil)
			})
		})
		Convey("add subscription has error", func() {
			id := vanus.NewID()
			tg.EXPECT().Init(gomock.Any()).Return(nil)
			tg.EXPECT().Start(gomock.Any()).Return(fmt.Errorf("error"))
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldNotBeNil)
			v, exist := m.getTrigger(id)
			So(exist, ShouldBeFalse)
			So(v, ShouldBeNil)
		})
	})
}

func TestRemoveSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("remove subscription", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tg := trigger.NewMockTrigger(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newTrigger = testNewTrigger(tg)

		Convey("remove no exist subscription", func() {
			v, exist := m.getTrigger(id)
			So(exist, ShouldBeFalse)
			So(v, ShouldBeNil)
			err := m.RemoveSubscription(ctx, id)
			So(err, ShouldBeNil)
		})
		Convey("remove exist subscription", func() {
			tg.EXPECT().Init(gomock.Any()).Return(nil)
			tg.EXPECT().Start(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			v, exist := m.getTrigger(id)
			So(exist, ShouldBeTrue)
			So(v, ShouldNotBeNil)
			tg.EXPECT().Stop(gomock.Any()).Return(nil)
			err = m.RemoveSubscription(ctx, id)
			So(err, ShouldBeNil)
			v, exist = m.getTrigger(id)
			So(exist, ShouldBeFalse)
			So(v, ShouldBeNil)
		})
	})
}

func TestPauseStartSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("pause subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tg := trigger.NewMockTrigger(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newTrigger = testNewTrigger(tg)
		id := vanus.NewID()
		Convey("pause no exist subscription", func() {
			err := m.PauseSubscription(ctx, id)
			So(err, ShouldBeNil)
		})
		Convey("start no exist subscription", func() {
			err := m.StartSubscription(ctx, id)
			So(err, ShouldNotBeNil)
		})
		Convey("pause exist subscription", func() {
			tg.EXPECT().Init(gomock.Any()).AnyTimes().Return(nil)
			tg.EXPECT().Start(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			v, exist := m.getTrigger(id)
			So(exist, ShouldBeTrue)
			So(v, ShouldNotBeNil)
			tg.EXPECT().Stop(gomock.Any()).Return(nil)
			err = m.PauseSubscription(ctx, id)
			So(err, ShouldBeNil)
			v, exist = m.getTrigger(id)
			So(exist, ShouldBeTrue)
			So(v, ShouldNotBeNil)
			Convey("start pause subscription", func() {
				err = m.StartSubscription(ctx, id)
				So(err, ShouldBeNil)
				v, exist = m.getTrigger(id)
				So(exist, ShouldBeTrue)
				So(v, ShouldNotBeNil)
			})
		})
	})
}

func TestResetOffsetToTimestamp(t *testing.T) {
	ctx := context.Background()
	Convey("test reset offset to timestamp", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tg := trigger.NewMockTrigger(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newTrigger = testNewTrigger(tg)
		Convey("reset offset no exist subscription", func() {
			err := m.ResetOffsetToTimestamp(ctx, id, time.Now().Unix())
			So(err, ShouldNotBeNil)
		})
		Convey("reset offset exist subscription", func() {
			tg.EXPECT().Init(gomock.Any()).AnyTimes().Return(nil)
			tg.EXPECT().Start(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			tg.EXPECT().Stop(gomock.Any()).Return(nil)
			offsets := info.ListOffsetInfo{{EventLogID: vanus.NewID(), Offset: uint64(100)}}
			tg.EXPECT().ResetOffsetToTimestamp(gomock.Any(), gomock.Any()).Return(offsets, nil)
			triggerClient := controller.NewMockTriggerControllerClient(ctrl)
			m.client = triggerClient
			triggerClient.EXPECT().CommitOffset(gomock.Any(), gomock.Any()).Return(nil, nil)
			err = m.ResetOffsetToTimestamp(ctx, id, time.Now().Unix())
			So(err, ShouldBeNil)
		})
	})
}

func TestWorker_Stop(t *testing.T) {
	ctx := context.Background()
	Convey("start stop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		tg := trigger.NewMockTrigger(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newTrigger = testNewTrigger(tg)
		id := vanus.NewID()
		tg.EXPECT().Init(gomock.Any()).Return(nil)
		tg.EXPECT().Start(gomock.Any()).AnyTimes().Return(nil)
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		v, exist := m.getTrigger(id)
		So(exist, ShouldBeTrue)
		So(v, ShouldNotBeNil)
		triggerClient := controller.NewMockTriggerControllerClient(ctrl)
		m.client = triggerClient
		tg.EXPECT().Stop(gomock.Any()).AnyTimes().Return(nil)
		offsets := info.ListOffsetInfo{{EventLogID: vanus.NewID(), Offset: uint64(100)}}
		tg.EXPECT().GetOffsets(gomock.Any()).AnyTimes().Return(offsets)
		triggerClient.EXPECT().CommitOffset(gomock.Any(), gomock.Any()).Return(nil, nil)
		err = m.Stop(ctx)
		So(err, ShouldBeNil)
		v, exist = m.getTrigger(id)
		So(exist, ShouldBeFalse)
		So(v, ShouldBeNil)
	})
}

func TestWorker_Register(t *testing.T) {
	Convey("test register", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		addr := "test"
		m := NewWorker(Config{TriggerAddr: addr}).(*worker)
		triggerClient := controller.NewMockTriggerControllerClient(ctrl)
		m.client = triggerClient
		triggerClient.EXPECT().RegisterTriggerWorker(gomock.Any(), gomock.Any()).Return(nil, nil)
		err := m.Register(ctx)
		So(err, ShouldBeNil)
	})
}

func TestWorker_Unregister(t *testing.T) {
	Convey("test unregister", t, func() {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		addr := "test"
		m := NewWorker(Config{TriggerAddr: addr}).(*worker)
		triggerClient := controller.NewMockTriggerControllerClient(ctrl)
		m.client = triggerClient
		triggerClient.EXPECT().UnregisterTriggerWorker(gomock.Any(), gomock.Any()).Return(nil, nil)
		err := m.Unregister(ctx)
		So(err, ShouldBeNil)
	})
}
