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
	"sort"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	pbtrigger "github.com/linkall-labs/vanus/proto/pkg/trigger"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReportSubId(t *testing.T) {
	tWorker := NewTriggerWorkerByAddr("test")
	map1 := map[vanus.ID]struct{}{
		1: {}, 2: {},
	}
	tWorker.SetReportSubscription(map1)
	getMap := tWorker.GetReportSubscription()
	now := time.Now()
	time.Sleep(time.Millisecond)
	tWorker.SetReportSubscription(map[vanus.ID]struct{}{
		11: {},
	})
	Convey("test", t, func() {
		So(len(getMap), ShouldEqual, 2)
		So(getMap, ShouldResemble, map1)
		b := tWorker.GetLastHeartbeatTime().After(now)
		So(b, ShouldBeTrue)
	})
}

func TestAssignSubId(t *testing.T) {
	tWorker := NewTriggerWorkerByAddr("test")
	subscriptionIDs := []vanus.ID{1, 2}
	for _, id := range subscriptionIDs {
		tWorker.AddAssignSubscription(id)
	}
	assignSubIds := tWorker.GetAssignSubscription()
	tWorker.AddAssignSubscription(3)
	Convey("test", t, func() {
		So(len(assignSubIds), ShouldEqual, 2)
		var keys []vanus.ID
		for k := range assignSubIds {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] > keys[j]
		})
		sort.Slice(subscriptionIDs, func(i, j int) bool {
			return subscriptionIDs[i] > subscriptionIDs[j]
		})
		So(keys, ShouldResemble, subscriptionIDs)
	})
	tWorker.RemoveAssignSubscription(1)
	Convey("test", t, func() {
		assignSubIds = tWorker.GetAssignSubscription()
		So(len(assignSubIds), ShouldEqual, 2)
		var keys []vanus.ID
		for k := range assignSubIds {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] > keys[j]
		})
		expect := []vanus.ID{2, 3}
		sort.Slice(expect, func(i, j int) bool {
			return expect[i] > expect[j]
		})
		So(keys, ShouldResemble, expect)
	})
}

func TestTriggerWorkerStart(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tWorker := NewTriggerWorkerByAddr("test")
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("start", t, func() {
		client.EXPECT().Start(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.Start(ctx)
		So(err, ShouldBeNil)
	})
}

func TestTriggerWorkerStop(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tWorker := NewTriggerWorkerByAddr("test")
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("start", t, func() {
		client.EXPECT().Stop(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.Stop(ctx)
		So(err, ShouldBeNil)
	})
}

func TestTriggerWorkerAddSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tWorker := NewTriggerWorkerByAddr("test")
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("start", t, func() {
		client.EXPECT().AddSubscription(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.AddSubscription(ctx, &primitive.Subscription{})
		So(err, ShouldBeNil)
	})
}

func TestTriggerWorkerRemoveSubscription(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tWorker := NewTriggerWorkerByAddr("test")
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("start", t, func() {
		client.EXPECT().RemoveSubscription(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.RemoveSubscriptions(ctx, 1)
		So(err, ShouldBeNil)
	})
}

func TestTriggerWorkerClose(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	tWorker := NewTriggerWorkerByAddr("test")
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("test close", t, func() {
		err := tWorker.Close()
		So(err, ShouldBeNil)
	})
}
