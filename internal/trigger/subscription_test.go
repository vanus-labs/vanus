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
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/trigger/trigger"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/internal/trigger/offset"
	"github.com/linkall-labs/vanus/internal/trigger/reader"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSubscriptionWorker(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := reader.NewMockReader(ctrl)
	Convey("subscription worker", t, func() {
		id := vanus.NewID()
		subscription := &primitive.Subscription{
			ID: id,
			Offsets: info.ListOffsetInfo{
				{Offset: 1, EventLogID: 1},
			},
			Config: primitive.SubscriptionConfig{
				RateLimit: 1000,
			},
		}
		subscriptionOffset := offset.NewSubscriptionOffset(id)
		w := NewSubscriptionWorker(subscription, subscriptionOffset, Config{
			ControllerAddr: []string{"test"},
		}).(*subscriptionWorker)
		w.reader = r
		r.EXPECT().Start().AnyTimes().Return(nil)
		r.EXPECT().Close().AnyTimes().Return()
		So(w.IsStart(), ShouldBeFalse)
		err := w.Run(ctx)
		So(err, ShouldBeNil)
		So(w.IsStart(), ShouldBeTrue)
		now := time.Now()
		So(w.GetStopTime().IsZero(), ShouldBeTrue)
		w.Stop(ctx)
		So(w.GetStopTime().IsZero(), ShouldBeFalse)
		So(w.GetStopTime().After(now), ShouldBeTrue)
	})
}

func TestSubscriptionWorker_Change(t *testing.T) {
	Convey("test trigger worker change subscription", t, func() {
		ctx := context.Background()
		id := vanus.NewID()
		w := NewSubscriptionWorker(&primitive.Subscription{
			ID: id,
		}, offset.NewSubscriptionOffset(id), Config{
			ControllerAddr: []string{"test"},
		}).(*subscriptionWorker)
		w.trigger = trigger.NewTrigger(w.subscription, w.subscriptionOffset)
		Convey("change target", func() {
			err := w.Change(ctx, &primitive.Subscription{Sink: "test_sink"})
			So(err, ShouldBeNil)
		})
		Convey("change filter", func() {
			err := w.Change(ctx, &primitive.Subscription{Filters: []*primitive.SubscriptionFilter{
				{Exact: map[string]string{"test": "test"}},
			}})
			So(err, ShouldBeNil)
		})
		Convey("change transformation", func() {
			err := w.Change(ctx, &primitive.Subscription{InputTransformer: &primitive.InputTransformer{}})
			So(err, ShouldBeNil)
		})
	})
}
