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

package offset

import (
	"github.com/linkall-labs/vanus/internal/primitive/info"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestManager_RegisterSubscription(t *testing.T) {
	m := NewOffsetManager()
	Convey("register subscription", t, func() {
		Convey("subscription offset same", func() {
			subId := "subId"
			So(m.RegisterSubscription(subId), ShouldEqual, m.RegisterSubscription(subId))
		})
		Convey("subId not equals subscription offset not same", func() {
			So(m.RegisterSubscription("subId1"), ShouldNotEqual, m.RegisterSubscription("subId2"))
		})
	})

}

func TestManager_GetSubscription(t *testing.T) {
	m := NewOffsetManager()
	Convey("get subscription", t, func() {
		subId := "subId"
		Convey("get subscription nil", func() {
			So(m.GetSubscription(subId), ShouldBeNil)
		})
		subOffset := m.RegisterSubscription(subId)
		Convey("get subscription not nil", func() {
			So(m.GetSubscription(subId), ShouldNotBeNil)
		})
		Convey("get same register", func() {
			So(subOffset, ShouldEqual, m.GetSubscription(subId))
		})
		Convey("multi get same", func() {
			So(m.GetSubscription(subId), ShouldEqual, m.GetSubscription(subId))
		})
	})
}

func TestManager_RemoveSubscription(t *testing.T) {
	m := NewOffsetManager()
	Convey("remove subscription", t, func() {
		subId := "subId"
		m.RegisterSubscription(subId)
		Convey("multi remove register subscription", func() {
			So(m.GetSubscription(subId), ShouldNotBeNil)
			m.RemoveSubscription(subId)
			So(m.GetSubscription(subId), ShouldBeNil)
			So(m.GetSubscription(subId), ShouldBeNil)
		})
	})
}

func TestSubscriptionOffset(t *testing.T) {
	subOffset := &SubscriptionOffset{subId: "subId"}
	Convey("subscription offset", t, func() {
		offsetBegin := int64(1)
		offsetEnd := int64(100)
		for offset := offsetBegin; offset <= offsetEnd; offset++ {
			subOffset.EventReceive(info.OffsetInfo{
				EventLogId: "eventLog",
				Offset:     offset,
			})
		}
		commitEnd := offsetBegin + 10
		for offset := offsetBegin; offset <= commitEnd; offset++ {
			subOffset.EventCommit(info.OffsetInfo{
				EventLogId: "eventLog",
				Offset:     offset,
			})
		}
		commits := subOffset.GetCommit()
		So(1, ShouldEqual, len(commits))
		So(commitEnd, ShouldEqual, commits[0].Offset)
		offsetBegin = commitEnd
		commitEnd = offsetEnd
		for offset := offsetBegin; offset <= commitEnd; offset++ {
			subOffset.EventCommit(info.OffsetInfo{
				EventLogId: "eventLog",
				Offset:     offset,
			})
		}
		commits = subOffset.GetCommit()
		So(1, ShouldEqual, len(commits))
		So(offsetEnd, ShouldEqual, commits[0].Offset)
		commits = subOffset.GetCommit()
		So(1, ShouldEqual, len(commits))
		So(offsetEnd, ShouldEqual, commits[0].Offset)
	})
}
