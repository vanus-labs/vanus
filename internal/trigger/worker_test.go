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
	"sync"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/proto/pkg/controller"

	"github.com/linkall-labs/vanus/internal/primitive/info"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	. "github.com/smartystreets/goconvey/convey"
)

func testNewSubscriptionWorker(subWorker SubscriptionWorker) newSubscriptionWorker {
	return func(subscription *primitive.Subscription, subscriptionOffset *offset.SubscriptionOffset, config Config) SubscriptionWorker {
		return subWorker
	}
}
func TestAddSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("add subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{ControllerAddr: []string{"test"}}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		Convey("add subscription", func() {
			id := vanus.NewID()
			subWorker.EXPECT().Run(gomock.Any()).Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			Convey("update subscription", func() {
				subWorker.EXPECT().Change(gomock.Any(), gomock.Any()).Return(nil)
				err = m.AddSubscription(ctx, &primitive.Subscription{
					ID:   id,
					Sink: "http://localhost:8080",
				})
				So(err, ShouldBeNil)
			})
		})
		Convey("add subscription has error", func() {
			id := vanus.NewID()
			subWorker.EXPECT().Run(gomock.Any()).Return(fmt.Errorf("error"))
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldNotBeNil)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
			So(m.offsetManager.GetSubscription(id), ShouldBeNil)
		})
	})
}

func TestRemoveSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("remove subscription", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)

		Convey("remove no exist subscription", func() {
			err := m.RemoveSubscription(ctx, id)
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
		Convey("remove exist subscription", func() {
			subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().Stop(gomock.Any()).Return()
			err = m.RemoveSubscription(ctx, id)
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
	})
}

func TestPauseStartSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("pause subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
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
			subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().Stop(gomock.Any()).Return()
			err = m.PauseSubscription(ctx, id)
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			Convey("start pause subscription", func() {
				err = m.StartSubscription(ctx, id)
				So(err, ShouldBeNil)
				So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			})
		})

	})
}

func TestCleanSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("clean subscription by ID", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		Convey("clean no exist subscription", func() {
			m.cleanSubscription(ctx, id)
		})
		subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
		Convey("clean exist subscription", func() {
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			m.cleanSubscription(ctx, id)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
	})
}

func TestResetOffsetToTimestamp(t *testing.T) {
	ctx := context.Background()
	Convey("test reset offset to timestamp", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		Convey("reset offset no exist subscription", func() {
			err := m.ResetOffsetToTimestamp(ctx, id, time.Now().Unix())
			So(err, ShouldNotBeNil)
		})
		Convey("reset offset exist subscription", func() {
			subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			subWorker.EXPECT().Stop(gomock.Any()).Return()
			offsets := info.ListOffsetInfo{{EventLogID: vanus.NewID(), Offset: uint64(100)}}
			subWorker.EXPECT().ResetOffsetToTimestamp(gomock.Any(), gomock.Any()).Return(offsets, nil)
			triggerClient := controller.NewMockTriggerControllerClient(ctrl)
			m.client.leaderClient = triggerClient
			triggerClient.EXPECT().CommitOffset(gomock.Any(), gomock.Any()).Return(nil, nil)
			err = m.ResetOffsetToTimestamp(ctx, id, time.Now().Unix())
			So(err, ShouldBeNil)
		})
	})
}

func TestHeartbeat(t *testing.T) {
	ctx := context.Background()
	Convey("test heartbeat", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{HeartbeatPeriod: time.Millisecond * 10}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		heartBeatClient := controller.NewMockTriggerController_TriggerWorkerHeartbeatClient(ctrl)
		m.client.heartBeatClient = heartBeatClient
		heartBeatClient.EXPECT().Send(gomock.Any()).AnyTimes().Return(nil)
		heartBeatClient.EXPECT().CloseSend().Return(nil)
		subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		cancelCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.startHeartbeat(cancelCtx)
		}()
		time.Sleep(time.Second)
		cancel()
		wg.Wait()
	})
}

func TestManager_Stop(t *testing.T) {
	ctx := context.Background()
	Convey("start stop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewWorker(Config{}).(*worker)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		id := vanus.NewID()
		subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		So(m.getSubscriptionWorker(id), ShouldNotBeNil)
		subWorker.EXPECT().Stop(gomock.Any()).AnyTimes().Return()
		err = m.Stop(ctx)
		So(m.getSubscriptionWorker(id), ShouldBeNil)
	})
}
