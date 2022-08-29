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
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/trigger/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSaveTriggerWorker(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewTriggerWorkerStorage(kvClient).(*triggerWorkerStorage)
	id := "testID"
	Convey("create trigger worker", t, func() {
		kvClient.EXPECT().Set(ctx, s.getKey(id), gomock.Any()).Return(nil)
		err := s.SaveTriggerWorker(ctx, metadata.TriggerWorkerInfo{
			ID:   id,
			Addr: "test",
		})
		So(err, ShouldBeNil)
	})

	Convey("update trigger worker", t, func() {
		kvClient.EXPECT().Set(ctx, s.getKey(id), gomock.Any()).Return(nil)
		err := s.SaveTriggerWorker(ctx, metadata.TriggerWorkerInfo{
			ID:   id,
			Addr: "test",
		})
		So(err, ShouldBeNil)
	})
}

func TestGetTriggerWorker(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewTriggerWorkerStorage(kvClient).(*triggerWorkerStorage)
	id := "testID"
	Convey("get trigger worker", t, func() {
		expect := metadata.TriggerWorkerInfo{
			ID:   id,
			Addr: "test",
		}
		v, _ := json.Marshal(expect)
		kvClient.EXPECT().Get(ctx, s.getKey(id)).Return(v, nil)
		data, err := s.GetTriggerWorker(ctx, id)
		So(err, ShouldBeNil)
		So(data.Addr, ShouldEqual, expect.Addr)
	})
}

func TestDeleteTriggerWorker(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewTriggerWorkerStorage(kvClient).(*triggerWorkerStorage)
	id := "testID"
	Convey("delete trigger worker", t, func() {
		kvClient.EXPECT().Delete(ctx, s.getKey(id)).Return(nil)
		err := s.DeleteTriggerWorker(ctx, id)
		So(err, ShouldBeNil)
	})
}

func TestListTriggerWorker(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	kvClient := kv.NewMockClient(ctrl)
	s := NewTriggerWorkerStorage(kvClient).(*triggerWorkerStorage)
	id := "testID"
	Convey("list trigger worker", t, func() {
		expect := metadata.TriggerWorkerInfo{
			ID:   id,
			Addr: "test",
		}
		v, _ := json.Marshal(expect)
		kvClient.EXPECT().List(ctx, s.getKey("/")).Return([]kv.Pair{
			{Key: id, Value: v},
		}, nil)
		list, err := s.ListTriggerWorker(ctx)
		So(err, ShouldBeNil)
		So(len(list), ShouldEqual, 1)
		So(list[0].Addr, ShouldEqual, expect.Addr)
	})
}
