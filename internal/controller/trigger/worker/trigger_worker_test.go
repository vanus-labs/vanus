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
	"github.com/linkall-labs/vanus/internal/controller/trigger/info"
	. "github.com/smartystreets/goconvey/convey"
	"sort"
	"testing"
)

func TestTriggerWorker_ReportSubId(t *testing.T) {
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
	map1 := map[string]struct{}{
		"a": {}, "b": {},
	}
	tWorker.SetReportSubId(map1)
	getMap := tWorker.GetReportSubId()
	tWorker.SetReportSubId(map[string]struct{}{
		"aa": {},
	})
	Convey("test", t, func() {
		So(len(getMap), ShouldEqual, 2)
		So(getMap, ShouldResemble, map1)
	})
}

func TestTriggerWorker_AssignSubId(t *testing.T) {
	tWorker := NewTriggerWorker(info.NewTriggerWorkerInfo("test"))
	subIds := []string{"sub1", "sub2"}
	for _, subId := range subIds {
		tWorker.AddAssignSub(subId)
	}
	assignSubIds := tWorker.GetAssignSubIds()
	tWorker.AddAssignSub("sub3")
	Convey("test", t, func() {
		So(len(assignSubIds), ShouldEqual, 2)
		var keys []string
		for k := range assignSubIds {
			keys = append(keys, k)
		}
		sort.StringSlice(keys).Sort()
		So(keys, ShouldResemble, subIds)
	})
	tWorker.RemoveAssignSub("sub1")
	Convey("test", t, func() {
		assignSubIds = tWorker.GetAssignSubIds()
		So(len(assignSubIds), ShouldEqual, 2)
		var keys []string
		for k := range assignSubIds {
			keys = append(keys, k)
		}
		sort.StringSlice(keys).Sort()
		So(keys, ShouldResemble, []string{"sub2", "sub3"})
	})
}
