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

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/info"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateOffset(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewOffsetStorage(kvClient).(*offsetStorage)
	subID := vanus.ID(1)
	eventLogID := vanus.ID(1)
	offset := uint64(100)
	Convey("create offset", t, func() {
		kvClient.EXPECT().Create(ctx, s.getKey(subID, eventLogID), s.int64ToByteArr(offset)).Return(nil)
		err := s.CreateOffset(ctx, subID, info.OffsetInfo{
			EventLogID: eventLogID,
			Offset:     offset,
		})
		So(err, ShouldBeNil)
	})
}

func TestUpdateOffset(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewOffsetStorage(kvClient).(*offsetStorage)
	subID := vanus.ID(1)
	eventLogID := vanus.ID(1)
	offset := uint64(100)
	Convey("update offset", t, func() {
		kvClient.EXPECT().Update(ctx, s.getKey(subID, eventLogID), s.int64ToByteArr(offset)).Return(nil)
		err := s.UpdateOffset(ctx, subID, info.OffsetInfo{
			EventLogID: eventLogID,
			Offset:     offset,
		})
		So(err, ShouldBeNil)
	})
}

func TestGetOffset(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewOffsetStorage(kvClient).(*offsetStorage)
	subID := vanus.ID(1)
	eventLogID := vanus.ID(1)
	offset := uint64(100)
	Convey("get offset", t, func() {
		kvClient.EXPECT().List(ctx, s.getSubKey(subID)).Return([]kv.Pair{
			{Key: fmt.Sprintf("/test/%d", eventLogID), Value: s.int64ToByteArr(offset)},
		}, nil)
		offsets, err := s.GetOffsets(ctx, subID)
		So(err, ShouldBeNil)
		So(len(offsets), ShouldEqual, 1)
		So(offsets[0].Offset, ShouldEqual, offset)
		So(offsets[0].EventLogID, ShouldEqual, eventLogID)
	})
}

func TestDeleteOffset(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewOffsetStorage(kvClient).(*offsetStorage)
	subID := vanus.ID(1)
	Convey("delete offset", t, func() {
		kvClient.EXPECT().DeleteDir(ctx, s.getSubKey(subID)).Return(nil)
		err := s.DeleteOffset(ctx, subID)
		So(err, ShouldBeNil)
	})
}
