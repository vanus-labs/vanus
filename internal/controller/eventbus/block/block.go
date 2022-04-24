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
	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	rpcerr "github.com/linkall-labs/vsproto/pkg/errors"
	"strings"
	"sync"
	"time"
)

const (
	defaultBlockSize                = 64 * 1024 * 1024
	defaultBlockBufferSizePerVolume = 8
)

var (
	ErrVolumeNotFound = rpcerr.New("volume not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
)

type Allocator interface {
	Run(ctx context.Context, kvCli kv.Client) error
	Stop()
	Pick(ctx context.Context, num int) ([]*metadata.Block, error)
	Clean(ctx context.Context, blocks ...*metadata.Block)
}

func NewAllocator(selector VolumeSelector) Allocator {
	return &allocator{
		selector:          selector,
		volumeBlockBuffer: make(map[string]*skiplist.SkipList, 0),
	}
}

type allocator struct {
	selector VolumeSelector
	// key: volumeID, value: SkipList of *metadata.Block
	volumeBlockBuffer map[string]*skiplist.SkipList
	segmentMap        sync.Map
	kvClient          kv.Client
	mutex             sync.Mutex
	inflightBlocks    sync.Map
	cancel            func()
	cancelCtx         context.Context
}

func (mgr *allocator) Run(ctx context.Context, kvCli kv.Client) error {
	mgr.kvClient = kvCli
	pairs, err := mgr.kvClient.List(ctx, metadata.BlockKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		bl := &metadata.Block{}
		err := json.Unmarshal(pair.Value, bl)
		if err != nil {
			return err
		}
		l, exist := mgr.volumeBlockBuffer[bl.VolumeID.Key()]
		if !exist {
			l = skiplist.New(skiplist.String)
			mgr.volumeBlockBuffer[bl.VolumeID.Key()] = l
		}
		l.Set(bl.ID.Key(), bl)
		mgr.segmentMap.Store(bl.ID.Key(), bl)
	}
	mgr.cancelCtx, mgr.cancel = context.WithCancel(context.Background())
	go mgr.dynamicAllocateBlockTask()
	return nil
}

func (mgr *allocator) Pick(ctx context.Context, num int) ([]*metadata.Block, error) {
	mgr.mutex.Lock()
	defer mgr.mutex.Unlock()
	blockArr := make([]*metadata.Block, num)

	instances := mgr.selector.Select(ctx, 3, defaultBlockSize)
	if len(instances) == 0 {
		return nil, ErrVolumeNotFound
	}
	for idx := 0; idx < num; idx++ {
		ins := instances[idx]
		list := mgr.volumeBlockBuffer[instances[idx].ID().Key()]
		if list == nil {
			list = skiplist.New(skiplist.String)
			mgr.volumeBlockBuffer[instances[idx].ID().Key()] = list
		}
		var err error
		var block *metadata.Block
		if list.Len() == 0 {
			block, err = ins.CreateBlock(ctx, defaultBlockSize)
			if err != nil {
				return nil, err
			}
			if err = mgr.updateBlockInKV(ctx, ins.ID(), block); err != nil {
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
		blockArr[idx] = block
	}
	if err := mgr.addToInflightBlock(blockArr...); err != nil {
		// put Block back to buffer
		for idx := range blockArr {
			block := blockArr[idx]
			list := mgr.volumeBlockBuffer[block.VolumeID.Key()]
			list.Set(block.ID, block)
		}
		return nil, err
	}
	return blockArr, nil
}

func (mgr *allocator) Clean(ctx context.Context, blocks ...*metadata.Block) {
	// mgr.inflightBlocks.Delete(block.ID)
	// TODO
}

func (mgr *allocator) Stop() {
	mgr.cancel()
}

func (mgr *allocator) dynamicAllocateBlockTask() {
	ctx := context.Background()
	for {
		select {
		case <-mgr.cancelCtx.Done():
			log.Info(ctx, "the dynamic-allocate task exit", nil)
			return
		default:
		}
		for k, v := range mgr.volumeBlockBuffer {
			volumeID, _ := vanus.NewIDFromString(k)
			instance := mgr.selector.SelectByID(ctx, volumeID)
			if instance == nil {
				log.Warning(ctx, "need to allocate block, but no volume instance founded", map[string]interface{}{
					"volume_id": k,
				})
				continue
			}

			for v.Len() < defaultBlockBufferSizePerVolume {
				block, err := instance.CreateBlock(ctx, defaultBlockSize)
				if err != nil {
					log.Warning(ctx, "create block failed", map[string]interface{}{
						"volume_id":   k,
						"buffer_size": v.Len(),
					})
					break
				}
				if err = mgr.updateBlockInKV(ctx, instance.ID(), block); err != nil {
					log.Warning(ctx, "insert block medata to etcd failed", map[string]interface{}{
						"volume_id":   k,
						"block_id":    block.ID,
						log.KeyError:  err,
						"buffer_size": v.Len(),
					})
					break
				}
				v.Set(block.ID.Key(), block)
			}
		}
		time.Sleep(time.Second)
	}
}

func (mgr *allocator) updateBlockInKV(ctx context.Context, volumeID vanus.ID, block *metadata.Block) error {
	if block == nil {
		return nil
	}
	data, err := json.Marshal(block)
	if err != nil {
		return err
	}
	key := strings.Join([]string{metadata.BlockKeyPrefixInKVStore, volumeID.Key(), block.ID.Key()}, "/")
	return mgr.kvClient.Set(ctx, key, data)
}

func (mgr *allocator) addToInflightBlock(blocks ...*metadata.Block) error {
	//mgr.inflightBlocks.Store(block.ID, block)
	// TODO update to etcd
	return nil
}
