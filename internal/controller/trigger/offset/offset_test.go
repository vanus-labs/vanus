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
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestManager_Offset(t *testing.T) {
	storage, _ := storage.NewFakeStorage(primitive.KvStorageConfig{})
	m := NewOffsetManager(storage)

	Convey("offset", t, func() {
		subId := "subId"
		eventLog := "el"
		offset := int64(1)
		Convey("get offset is empty", func() {
			m.RemoveRegisterSubscription(nil, subId)
			offsets, _ := m.GetOffset(nil, subId)
			So(len(offsets), ShouldEqual, 0)
		})

		Convey("get offset", func() {
			m.RemoveRegisterSubscription(nil, subId)
			storage.CreateOffset(nil, subId, info.OffsetInfo{
				EventLog: eventLog,
				Offset:   offset,
			})
			offsets, _ := m.GetOffset(nil, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)

		})

		Convey("offset", func() {
			m.RemoveRegisterSubscription(nil, subId)
			m.Offset(nil, info.SubscriptionInfo{
				SubId:   subId,
				Offsets: []info.OffsetInfo{{EventLog: eventLog, Offset: offset}},
			})
			offsets, _ := m.GetOffset(nil, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
		})

		Convey("commit", func() {
			m.RemoveRegisterSubscription(nil, subId)
			m.Offset(nil, info.SubscriptionInfo{
				SubId:   subId,
				Offsets: []info.OffsetInfo{{EventLog: eventLog, Offset: offset}},
			})
			offsets, _ := m.GetOffset(nil, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
			m.Run()

			m.Stop()
		})
	})
}

func TestManager_Start(t *testing.T) {
	storage, _ := storage.NewFakeStorage(primitive.KvStorageConfig{})
	m := NewOffsetManager(storage)
	Convey("commit", t, func() {
		subId := "subId"
		eventLog := "el"
		offset := int64(1)
		Convey("commit", func() {
			m.Offset(nil, info.SubscriptionInfo{
				SubId:   subId,
				Offsets: []info.OffsetInfo{{EventLog: eventLog, Offset: offset}},
			})
			offsets, _ := m.GetOffset(nil, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
			offsets, _ = storage.GetOffsets(nil, subId)
			So(len(offsets), ShouldEqual, 0)
			m.Run()
			time.Sleep(time.Second)
			offsets, _ = storage.GetOffsets(nil, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
			m.Stop()
		})
	})
}
