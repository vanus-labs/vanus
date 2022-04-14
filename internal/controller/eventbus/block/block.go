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
	"context"
	"encoding/json"
	"fmt"
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/pkg/errors"
	"strings"
	"sync"
)

const (
	defaultBlockSize        = 64 * 1024 * 1024
	blockKeyPrefixInKVStore = "/vanus/internal/resource/Block"
)

var (
	ErrVolumeNotFound = errors.New("volume not found")
)

type Allocator interface {
	Init(ctx context.Context, kvCli kv.Client) error
	Pick(ctx context.Context, num int, size int64) ([]*metadata.Block, error)
	Remove(ctx context.Context, seg *metadata.Block) error
}

func NewAllocator(selector VolumeSelector) Allocator {
	return &allocator{
		selector: selector,
	}
}

type allocator struct {
	selector VolumeSelector
	// key: volumeID, value: SkipList of *metadata.Block
	volumeBlockBuffer map[uint64]*skiplist.SkipList
	segmentMap        sync.Map
	kvClient          kv.Client
	mutex             sync.Mutex
	// key: blockID, value: block
	inflightBlocks sync.Map
}

func (mgr *allocator) Init(ctx context.Context, kvCli kv.Client) error {
	mgr.kvClient = kvCli
	go mgr.dynamicAllocateSegmentTask()
	//mgr.primitive = NewVolumeRoundRobin(mgr.volumeMgr.GetAllVolume)
	pairs, err := mgr.kvClient.List(ctx, blockKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	// TODO unassigned -> assigned
	for idx := range pairs {
		pair := pairs[idx]
		bl := &metadata.Block{}
		err := json.Unmarshal(pair.Value, bl)
		if err != nil {
			return err
		}
		l, exist := mgr.volumeBlockBuffer[bl.VolumeID]
		if !exist {
			l = skiplist.New(skiplist.String)
			mgr.volumeBlockBuffer[bl.VolumeID] = l
		}
		l.Set(bl.ID, bl)
		mgr.segmentMap.Store(bl.ID, bl)
	}
	return nil
}

func (mgr *allocator) Pick(ctx context.Context, num int, size int64) ([]*metadata.Block, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	blockArr := make([]*metadata.Block, num)

	instances := mgr.selector.Select(ctx, 3, size)
	if len(instances) == 0 {
		return nil, ErrVolumeNotFound
	}
	for idx := 0; idx < num; idx++ {
		ins := instances[idx]
		list := mgr.volumeBlockBuffer[instances[idx].ID()]
		if list == nil {
			list = skiplist.New(skiplist.Uint64)
			mgr.volumeBlockBuffer[instances[idx].ID()] = list
		}
		var err error
		var block *metadata.Block
		if list.Len() == 0 {
			block, err = ins.CreateBlock(ctx, size)
			if err != nil {
				return nil, err
			}
			if err = mgr.updateBlockInKV(ctx, block); err != nil {
				log.Error(ctx, "save block metadata to kv failed after creating", map[string]interface{}{
					log.KeyError: err,
					"block":      block,
				})
				return nil, err
			}
		} else {
			val := list.RemoveFront()
			block = val.Value.(*metadata.Block)
		}
		blockArr = append(blockArr, block)
	}
	if err := mgr.addToInflightBlock(blockArr...); err != nil {
		// put Block back to buffer
		for idx := range blockArr {
			block := blockArr[idx]
			list := mgr.volumeBlockBuffer[block.VolumeID]
			list.Set(block.ID, block)
		}
		return nil, err
	}
	return blockArr, nil
}

func (mgr *allocator) Remove(ctx context.Context, block *metadata.Block) error {
	//ins := mgr.volumeMgr.GetVolumeInstanceByID(block.VolumeID)
	//if ins == nil {
	//	return ErrVolumeNotFound
	//}
	//TODO
	return nil
}

func (mgr *allocator) destroy() error {
	return nil
}

func (mgr *allocator) dynamicAllocateSegmentTask() {
	//TODO
}

func (mgr *allocator) getBlockKeyInKVStore(blockID uint64) string {
	return strings.Join([]string{blockKeyPrefixInKVStore, fmt.Sprintf("%d", blockID)}, "/")
}

func (mgr *allocator) updateBlockInKV(ctx context.Context, block *metadata.Block) error {
	if block == nil {
		return nil
	}
	data, err := json.Marshal(block)
	if err != nil {
		return err
	}
	return mgr.kvClient.Set(ctx, mgr.getBlockKeyInKVStore(block.ID), data)
}

func (mgr *allocator) addToInflightBlock(blocks ...*metadata.Block) error {
	//mgr.inflightBlocks.Store(block.ID, block)
	// TODO update to etcd
	return nil
}
