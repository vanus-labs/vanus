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

	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	pbtrigger "github.com/linkall-labs/vsproto/pkg/trigger"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestReportSubId(t *testing.T) {
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
	map1 := map[vanus.ID]struct{}{
		1: {}, 2: {},
	}
	tWorker.SetReportSubId(map1)
	getMap := tWorker.GetReportSubId()
	now := time.Now()
	time.Sleep(time.Millisecond)
	tWorker.SetReportSubId(map[vanus.ID]struct{}{
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
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
	subIds := []vanus.ID{1, 2}
	for _, subId := range subIds {
		tWorker.AddAssignSub(subId)
	}
	assignSubIds := tWorker.GetAssignSubIds()
	tWorker.AddAssignSub(3)
	Convey("test", t, func() {
		So(len(assignSubIds), ShouldEqual, 2)
		var keys []vanus.ID
		for k := range assignSubIds {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] > keys[j]
		})
		sort.Slice(subIds, func(i, j int) bool {
			return subIds[i] > subIds[j]
		})
		So(keys, ShouldResemble, subIds)
	})
	tWorker.RemoveAssignSub(1)
	Convey("test", t, func() {
		assignSubIds = tWorker.GetAssignSubIds()
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
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
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
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
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
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
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
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
	_ = tWorker.init(ctx)
	client := pbtrigger.NewMockTriggerWorkerClient(ctrl)
	tWorker.client = client
	Convey("start", t, func() {
		client.EXPECT().RemoveSubscription(ctx, gomock.Any()).Return(nil, nil)
		err := tWorker.RemoveSubscriptions(ctx, 1)
		So(err, ShouldBeNil)
	})
}
