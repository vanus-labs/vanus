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
	stdJson "encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"

	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAllocator_Pick(t *testing.T) {
	Convey("test pick method of allocator.Pick", t, func() {
		ctrl := gomock.NewController(t)
		alloc := getAllocator(ctrl)
		kvMock := kv.NewMockClient(ctrl)
		alloc.kvClient = kvMock
		kvMock.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(nil)
		Convey("get 1 block", func() {
			blocks, err := alloc.Pick(stdCtx.Background(), 1)
			So(err, ShouldBeNil)
			So(blocks, ShouldHaveLength, 1)
		})

		Convey("get 3 blocks", func() {
			blocks, err := alloc.Pick(stdCtx.Background(), 3)
			So(err, ShouldBeNil)
			So(blocks, ShouldHaveLength, 3)
		})
	})
}

func TestAllocator_RunWithoutDynamic(t *testing.T) {
	Convey("test run without dynamic allocate block", t, func() {
		ctrl := gomock.NewController(t)
		alloc := getAllocator(ctrl)
		kvMock := kv.NewMockClient(ctrl)
		alloc.kvClient = kvMock

		ctx := stdCtx.Background()
		block1 := metadata.Block{
			ID:         vanus.NewID(),
			Capacity:   defaultBlockSize,
			VolumeID:   vanus.NewIDFromUint64(1),
			EventlogID: vanus.NewID(),
			SegmentID:  vanus.NewID(),
		}
		block2 := metadata.Block{
			ID:         vanus.NewID(),
			Capacity:   defaultBlockSize,
			VolumeID:   vanus.NewIDFromUint64(2),
			EventlogID: vanus.NewID(),
			SegmentID:  vanus.NewID(),
		}
		block3 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: defaultBlockSize,
			VolumeID: vanus.NewIDFromUint64(3),
		}
		block4 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: defaultBlockSize,
			VolumeID: vanus.NewIDFromUint64(1),
		}
		block5 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: defaultBlockSize,
			VolumeID: vanus.NewIDFromUint64(2),
		}
		block6 := metadata.Block{
			ID:       vanus.NewID(),
			Capacity: defaultBlockSize,
			VolumeID: vanus.NewIDFromUint64(3),
		}
		data1, _ := stdJson.Marshal(block1)
		data2, _ := stdJson.Marshal(block2)
		data3, _ := stdJson.Marshal(block3)
		data4, _ := stdJson.Marshal(block4)
		data5, _ := stdJson.Marshal(block5)
		data6, _ := stdJson.Marshal(block6)
		kvMock.EXPECT().List(ctx, metadata.BlockKeyPrefixInKVStore).Return([]kv.Pair{
			{
				Key:   metadata.GetBlockMetadataKey(block1.VolumeID, block1.ID),
				Value: data1,
			},
			{
				Key: strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
					block2.VolumeID.String(), block2.ID.Key()}, ","),
				Value: data2,
			},
			{
				Key: strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
					block3.VolumeID.String(), block3.ID.Key()}, ","),
				Value: data3,
			},
			{
				Key: strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
					block4.VolumeID.String(), block4.ID.Key()}, ","),
				Value: data4,
			},
			{
				Key: strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
					block5.VolumeID.String(), block5.ID.Key()}, ","),
				Value: data5,
			},
			{
				Key: strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
					block6.VolumeID.String(), block6.ID.Key()}, ","),
				Value: data6,
			},
		}, nil)

		Convey("test recovery store to memory from etcd", func() {
			err := alloc.Run(ctx, kvMock, false)
			So(err, ShouldBeNil)
			v, exist := alloc.volumeBlockBuffer.Load(block1.VolumeID.String())
			So(exist, ShouldBeTrue)
			list1 := v.(*skiplist.SkipList)
			v, exist = alloc.volumeBlockBuffer.Load(block2.VolumeID.String())
			So(exist, ShouldBeTrue)
			list2 := v.(*skiplist.SkipList)
			v, exist = alloc.volumeBlockBuffer.Load(block3.VolumeID.String())
			So(exist, ShouldBeTrue)
			list3 := v.(*skiplist.SkipList)

			So(list1.Len(), ShouldEqual, 1)
			So(list2.Len(), ShouldEqual, 1)
			So(list3.Len(), ShouldEqual, 2)

			So(list1.Get(block1.ID.String()), ShouldBeNil)
			So(list2.Get(block2.ID.String()), ShouldBeNil)
			So(list3.Get(block3.ID.String()).Value, ShouldResemble, &block3)
			So(list1.Get(block4.ID.String()).Value, ShouldResemble, &block4)
			So(list2.Get(block5.ID.String()).Value, ShouldResemble, &block5)
			So(list3.Get(block6.ID.String()).Value, ShouldResemble, &block6)
		})
	})
}

func TestAllocator_RunWithDynamic(t *testing.T) {
	Convey("test run without dynamic allocate block", t, func() {
		ctrl := gomock.NewController(t)
		srv1 := server.NewMockInstance(ctrl)
		srv1.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
			ID:       vanus.NewIDFromUint64(1),
			Capacity: 64 * 1024 * 1024,
		})
		srv1.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
		srv1.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			size int64) (*metadata.Block, error) {
			return &metadata.Block{
				ID:       vanus.NewID(),
				Capacity: size,
				VolumeID: vanus.NewIDFromUint64(1),
			}, nil
		})

		srv2 := server.NewMockInstance(ctrl)
		srv2.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
			ID:       vanus.NewIDFromUint64(2),
			Capacity: 64 * 1024 * 1024,
		})
		srv2.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
		srv2.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			size int64) (*metadata.Block, error) {
			return &metadata.Block{
				ID:       vanus.NewID(),
				Capacity: size,
				VolumeID: vanus.NewIDFromUint64(2),
			}, nil
		})

		srv3 := server.NewMockInstance(ctrl)
		srv3.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
			ID:       vanus.NewIDFromUint64(3),
			Capacity: 64 * 1024 * 1024,
		})
		srv3.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
		srv3.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
			size int64) (*metadata.Block, error) {
			return &metadata.Block{
				ID:       vanus.NewID(),
				Capacity: size,
				VolumeID: vanus.NewIDFromUint64(3),
			}, nil
		})

		mutex := sync.Mutex{}
		var instanceList []server.Instance
		alloc := NewAllocator(0, NewVolumeRoundRobin(func() []server.Instance {
			mutex.Lock()
			defer mutex.Unlock()
			return instanceList
		})).(*allocator)
		kvMock := kv.NewMockClient(ctrl)
		alloc.kvClient = kvMock

		ctx := stdCtx.Background()
		kvMock.EXPECT().List(ctx, metadata.BlockKeyPrefixInKVStore).Return(nil, nil)

		alloc.allocateTicker = time.NewTicker(10 * time.Millisecond)
		kvMock.EXPECT().Set(gomock.Any(), gomock.Any(), gomock.Any()).Times(24)
		err := alloc.Run(ctx, kvMock, true)
		So(err, ShouldBeNil)
		count := 0
		alloc.volumeBlockBuffer.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		So(count, ShouldBeZeroValue)
		mutex.Lock()
		instanceList = []server.Instance{srv1, srv2, srv3}
		mutex.Unlock()
		time.Sleep(time.Second)
		v, exist := alloc.volumeBlockBuffer.Load("1")
		So(exist, ShouldBeTrue)
		list1 := v.(*skiplist.SkipList)
		v, exist = alloc.volumeBlockBuffer.Load("2")
		So(exist, ShouldBeTrue)
		list2 := v.(*skiplist.SkipList)
		v, exist = alloc.volumeBlockBuffer.Load("3")
		So(exist, ShouldBeTrue)
		list3 := v.(*skiplist.SkipList)

		So(list1.Len(), ShouldEqual, 8)
		So(list2.Len(), ShouldEqual, 8)
		So(list3.Len(), ShouldEqual, 8)
	})
}

func TestAllocator_UpdateBlock(t *testing.T) {
	Convey("update update block in kv", t, func() {
		ctrl := gomock.NewController(t)
		alloc := getAllocator(ctrl)
		kvMock := kv.NewMockClient(ctrl)
		alloc.kvClient = kvMock
		ctx := stdCtx.Background()
		block := &metadata.Block{
			ID:       vanus.NewID(),
			VolumeID: vanus.NewID(),
			Capacity: defaultBlockSize,
		}
		data, _ := stdJson.Marshal(block)
		key := strings.Join([]string{metadata.BlockKeyPrefixInKVStore, block.VolumeID.Key(), block.ID.Key()}, "/")
		kvMock.EXPECT().Set(ctx, key, data).Return(nil)
		err := alloc.updateBlockInKV(ctx, block)
		So(err, ShouldBeNil)
	})
}

func getAllocator(ctrl *gomock.Controller) *allocator {
	srv1 := server.NewMockInstance(ctrl)
	srv1.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(1),
		Capacity: 64 * 1024 * 1024,
	})
	srv1.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv1.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
		size int64) (*metadata.Block, error) {
		return &metadata.Block{
			ID:       vanus.NewID(),
			Capacity: size,
			VolumeID: vanus.NewIDFromUint64(1),
		}, nil
	})

	srv2 := server.NewMockInstance(ctrl)
	srv2.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(2),
		Capacity: 64 * 1024 * 1024,
	})
	srv2.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv2.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
		size int64) (*metadata.Block, error) {
		return &metadata.Block{
			ID:       vanus.NewID(),
			Capacity: size,
			VolumeID: vanus.NewIDFromUint64(2),
		}, nil
	})

	srv3 := server.NewMockInstance(ctrl)
	srv3.EXPECT().GetMeta().AnyTimes().Return(&metadata.VolumeMetadata{
		ID:       vanus.NewIDFromUint64(3),
		Capacity: 64 * 1024 * 1024,
	})
	srv3.EXPECT().ID().AnyTimes().Return(vanus.NewIDFromUint64(uint64(time.Now().UnixNano())))
	srv3.EXPECT().CreateBlock(gomock.Any(), defaultBlockSize).AnyTimes().DoAndReturn(func(ctx stdCtx.Context,
		size int64) (*metadata.Block, error) {
		return &metadata.Block{
			ID:       vanus.NewID(),
			Capacity: size,
			VolumeID: vanus.NewIDFromUint64(3),
		}, nil
	})

	alloc := NewAllocator(0, NewVolumeRoundRobin(func() []server.Instance {
		return []server.Instance{srv1, srv2, srv3}
	}))
	return alloc.(*allocator)
}
