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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/errors"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAddSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("add subscription", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
		m.startSubscription = false
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		_, exist := m.subscriptionMap.Load(id)
		So(exist, ShouldBeTrue)
		Convey("repeat add subscription", func() {
			err = m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, errors.ErrResourceAlreadyExist)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reader.NewMockReader(ctrl)
	Convey("remove subscription", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
		Convey("remove no exist subscription", func() {
			m.RemoveSubscription(ctx, id)
		})
		Convey("remove exist subscription", func() {
			m.startSubscription = false
			err := m.AddSubscription(ctx, &primitive.Subscription{
				ID: id,
			})
			So(err, ShouldBeNil)
			worker := m.getSubscriptionWorker(id)
			worker.reader = r
			r.EXPECT().Start().AnyTimes().Return(nil)
			r.EXPECT().Close().AnyTimes().Return()
			err = worker.Run(ctx)
			So(err, ShouldBeNil)
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
			err = m.RemoveSubscription(ctx, id)
			cancel()
			So(err, ShouldBeNil)
			worker = m.getSubscriptionWorker(id)
			So(worker, ShouldBeNil)
		})
	})
}

func TestPauseSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("pause subscription", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
		m.startSubscription = false
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		err = m.PauseSubscription(ctx, id)
		So(err, ShouldBeNil)
		_, exist := m.subscriptionMap.Load(id)
		So(exist, ShouldBeTrue)
	})
}

func TestCleanSubscription(t *testing.T) {
	ctx := context.Background()
	Convey("clean subscription by ID", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{CleanSubscriptionTimeout: time.Millisecond * 100}).(*manager)
		Convey("clean no exist subscription", func() {
			m.cleanSubscription(ctx, id)
		})
		Convey("clean no start subscription", func() {
			m.subscriptionMap.Store(id, &subscriptionWorker{})
			m.cleanSubscription(ctx, id)
		})
		Convey("clean exist subscription timeout", func() {
			now := time.Now()
			m.subscriptionMap.Store(id, &subscriptionWorker{
				stopTime:  now,
				startTime: &now,
			})
			m.cleanSubscription(ctx, id)
			_, exist := m.subscriptionMap.Load(id)
			So(exist, ShouldBeFalse)
		})
	})
}

func TestManager_Stop(t *testing.T) {
	ctx := context.Background()
	Convey("start stop", t, func() {
		id := vanus.NewID()
		m := NewManager(Config{Controllers: []string{"test"}}).(*manager)
		m.startSubscription = false
		err := m.AddSubscription(ctx, &primitive.Subscription{
			ID: id,
		})
		So(err, ShouldBeNil)
		m.Stop(ctx)
	})
}
