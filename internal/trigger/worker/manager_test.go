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

package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/trigger/offset"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
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
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
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

func TestListSubscriptionInfo(t *testing.T) {
	Convey("list subscription info", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
		m.subscriptionMap.Store(id, &subscriptionWorker{
			subscription: &primitive.Subscription{
				ID: id,
			},
		})
		Convey("list subscription unregister offset", func() {
			list, _ := m.ListSubscriptionInfo()
			So(len(list), ShouldEqual, 0)
		})
		Convey("list subscription register offset", func() {
			m.offsetManager.RegisterSubscription(id)
			list, _ := m.ListSubscriptionInfo()
			So(len(list), ShouldEqual, 1)
			So(list[0].SubscriptionID, ShouldEqual, id)
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
		m := NewManager(Config{}).(*manager)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)

		Convey("remove no exist subscription", func() {
			m.RemoveSubscription(ctx, id)
		})
		Convey("remove exist subscription", func() {
			subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().Stop(gomock.Any()).AnyTimes().Return()
			subWorker.EXPECT().IsStart().Return(false)
			err = m.RemoveSubscription(ctx, id)
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
	})
}

func TestPauseSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("pause subscription", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewManager(Config{}).(*manager)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		id := vanus.NewID()
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
	})
}

func TestCleanSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("clean subscription by ID", t, func() {
		id := vanus.NewID()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewManager(Config{CleanSubscriptionTimeout: time.Millisecond * 100}).(*manager)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		Convey("clean no exist subscription", func() {
			m.cleanSubscription(ctx, id)
		})
		subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
		Convey("clean no start subscription", func() {
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().IsStart().Return(false)
			m.cleanSubscription(ctx, id)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
		Convey("clean exist subscription timeout", func() {
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().IsStart().Return(true)
			now := time.Now()
			subWorker.EXPECT().GetStopTime().AnyTimes().Return(now)
			m.cleanSubscription(ctx, id)
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})

		Convey("clean exist subscription stopTime gt last commitTime", func() {
			m.config.CleanSubscriptionTimeout = time.Hour
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			So(m.getSubscriptionWorker(id), ShouldNotBeNil)
			subWorker.EXPECT().IsStart().Return(true)
			now := time.Now()
			subWorker.EXPECT().GetStopTime().AnyTimes().Return(now.Add(time.Second))
			ctxCancel, cancel := context.WithCancel(context.Background())
			go func() {
				ticker := time.NewTicker(time.Millisecond * 10)
				for {
					select {
					case <-ctxCancel.Done():
						return
					case <-ticker.C:
						_, f := m.ListSubscriptionInfo()
						f()
					}
				}
			}()
			m.cleanSubscription(ctx, id)
			cancel()
			So(m.getSubscriptionWorker(id), ShouldBeNil)
		})
	})
}

func TestManager_Stop(t *testing.T) {
	ctx := context.Background()
	Convey("start stop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		subWorker := NewMockSubscriptionWorker(ctrl)
		m := NewManager(Config{}).(*manager)
		m.newSubscriptionWorker = testNewSubscriptionWorker(subWorker)
		id := vanus.NewID()
		subWorker.EXPECT().Run(gomock.Any()).AnyTimes().Return(nil)
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		So(m.getSubscriptionWorker(id), ShouldNotBeNil)
		subWorker.EXPECT().Stop(gomock.Any()).AnyTimes().Return()
		subWorker.EXPECT().IsStart().Return(false)
		m.Stop(ctx)
		So(m.getSubscriptionWorker(id), ShouldBeNil)
	})
}
