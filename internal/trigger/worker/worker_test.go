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
		}
		offsetManager := offset.NewOffsetManager()
		offsetManager.RemoveSubscription(id)
		subscriptionOffset := offsetManager.GetSubscription(id)
		w := NewSubscriptionWorker(subscription, subscriptionOffset,
			[]string{"test"}).(*subscriptionWorker)
		w.reader = r
		r.EXPECT().Start().AnyTimes().Return(nil)
		r.EXPECT().Close().AnyTimes().Return()
		err := w.Run(ctx)
		So(err, ShouldBeNil)
		w.Stop(ctx)
	})
}
