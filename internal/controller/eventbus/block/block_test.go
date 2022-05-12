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

package block

import (
	stdCtx "context"
	"github.com/golang/mock/gomock"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"testing"
	"time"

	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAllocator_Pick(t *testing.T) {
	Convey("test pick method of allocator.Pick", t, func() {
		ctrl := gomock.NewController(t)
		alloc := getSelector(ctrl)
		allocObj := alloc.(*allocator)
		kvMock := kv.NewMockClient(ctrl)
		allocObj.kvClient = kvMock
		kvMock.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		Convey("get 1 block", func() {
			blocks, err := alloc.Pick(stdCtx.Background(), 1)
			So(err, ShouldBeNil)
			So(blocks, ShouldHaveLength, 1)
		})
	})
}

func TestAllocator_Run(t *testing.T) {
	Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		srv1 := server.NewMockInstance(ctrl)
		srv2 := server.NewMockInstance(ctrl)
		srv3 := server.NewMockInstance(ctrl)
		_ = NewVolumeRoundRobin(func() []server.Instance {
			return []server.Instance{srv1, srv2, srv3}
		})

	})
}

func TestAllocator_Stop(t *testing.T) {
	Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		srv1 := server.NewMockInstance(ctrl)
		srv2 := server.NewMockInstance(ctrl)
		srv3 := server.NewMockInstance(ctrl)
		_ = NewVolumeRoundRobin(func() []server.Instance {
			return []server.Instance{srv1, srv2, srv3}
		})

	})
}

func TestAllocator_Clean(t *testing.T) {
	Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		srv1 := server.NewMockInstance(ctrl)
		srv2 := server.NewMockInstance(ctrl)
		srv3 := server.NewMockInstance(ctrl)
		_ = NewVolumeRoundRobin(func() []server.Instance {
			return []server.Instance{srv1, srv2, srv3}
		})

	})
}

func getSelector(ctrl *gomock.Controller) Allocator {
	srv1 := server.NewMockInstance(ctrl)
	srv1.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(1),
		Capacity: 64 * 1024 * 1024,
	})
	srv1.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv1.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().Return(&metadata.Block{
		ID:       vanus.NewID(),
		Capacity: defaultBlockSize,
		VolumeID: vanus.NewIDFromUint64(1),
	}, nil)

	srv2 := server.NewMockInstance(ctrl)
	srv2.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(2),
		Capacity: 64 * 1024 * 1024,
	})
	srv2.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv2.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().Return(&metadata.Block{
		ID:       vanus.NewID(),
		Capacity: defaultBlockSize,
		VolumeID: vanus.NewIDFromUint64(2),
	}, nil)

	srv3 := server.NewMockInstance(ctrl)
	srv3.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(3),
		Capacity: 64 * 1024 * 1024,
	})
	srv3.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv3.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().Return(&metadata.Block{
		ID:       vanus.NewID(),
		Capacity: defaultBlockSize,
		VolumeID: vanus.NewIDFromUint64(3),
	}, nil)
	alloc := NewAllocator(NewVolumeRoundRobin(func() []server.Instance {
		return []server.Instance{srv1, srv2, srv3}
	}))
	return alloc
}
