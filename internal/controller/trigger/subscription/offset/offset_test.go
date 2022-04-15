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
	"context"
	"github.com/linkall-labs/vanus/internal/controller/trigger/storage"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestOffset(t *testing.T) {
	storage := storage.NewFakeStorage()
	m := NewOffsetManager(storage, 10*time.Microsecond)
	ctx := context.Background()
	Convey("offset", t, func() {
		subId := vanus.ID(1)
		eventLogID := vanus.ID(1)
		offset := uint64(1)
		Convey("get offset is empty", func() {
			m.RemoveRegisterSubscription(ctx, subId)
			offsets, _ := m.GetOffset(ctx, subId)
			So(len(offsets), ShouldEqual, 0)
		})

		Convey("get offset", func() {
			m.RemoveRegisterSubscription(ctx, subId)
			storage.CreateOffset(ctx, subId, info.OffsetInfo{
				EventLogID: eventLogID,
				Offset:     offset,
			})
			offsets, _ := m.GetOffset(ctx, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)

		})

		Convey("offset", func() {
			m.RemoveRegisterSubscription(ctx, subId)
			m.Offset(ctx, subId, []info.OffsetInfo{{EventLogID: eventLogID, Offset: offset}})
			offsets, _ := m.GetOffset(ctx, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
		})

		Convey("commit", func() {
			m.RemoveRegisterSubscription(ctx, subId)
			m.Offset(ctx, subId, []info.OffsetInfo{{EventLogID: eventLogID, Offset: offset}})
			offsets, _ := m.GetOffset(ctx, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
			m.Start()
			m.Stop()
		})
	})
}

func TestStart(t *testing.T) {
	ctx := context.Background()
	storage := storage.NewFakeStorage()
	commitInterval := 10 * time.Millisecond
	m := NewOffsetManager(storage, commitInterval)
	Convey("commit", t, func() {
		subId := vanus.ID(1)
		eventLogID := vanus.ID(1)
		offset := uint64(1)
		m.Offset(ctx, subId, []info.OffsetInfo{{EventLogID: eventLogID, Offset: offset}})
		Convey("commit storage created", func() {
			offsets, _ := m.GetOffset(ctx, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)
			offsets, _ = storage.GetOffsets(ctx, subId)
			So(len(offsets), ShouldEqual, 0)
			m.Start()
			time.Sleep(2 * commitInterval)
			m.Stop()
			offsets, _ = storage.GetOffsets(ctx, subId)
			So(len(offsets), ShouldEqual, 1)
			So(offsets[0].Offset, ShouldEqual, offset)

			offset++
			m.Offset(ctx, subId, []info.OffsetInfo{{EventLogID: eventLogID, Offset: offset}})
			Convey("commit storage update", func() {
				offsets, _ = m.GetOffset(ctx, subId)
				So(len(offsets), ShouldEqual, 1)
				So(offsets[0].Offset, ShouldEqual, offset)
				offsets, _ = storage.GetOffsets(ctx, subId)
				So(len(offsets), ShouldEqual, 1)
				m.Start()
				time.Sleep(2 * commitInterval)
				m.Stop()
				offsets, _ = storage.GetOffsets(ctx, subId)
				So(len(offsets), ShouldEqual, 1)
				So(offsets[0].Offset, ShouldEqual, offset)
			})
		})

	})
}
