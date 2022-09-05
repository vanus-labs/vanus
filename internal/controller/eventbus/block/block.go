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
	"strings"
	"sync"
	"time"

	"github.com/huandu/skiplist"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	rpcerr "github.com/linkall-labs/vanus/proto/pkg/errors"
)

const (
	defaultBlockSize                = int64(64 * 1024 * 1024)
	minimumBlockSize                = int64(4 * 1024 * 1024)
	defaultBlockBufferSizePerVolume = 8
)

var (
	ErrVolumeNotFound = rpcerr.New("volume not found").WithGRPCCode(rpcerr.ErrorCode_RESOURCE_NOT_FOUND)
)

type Allocator interface {
	Run(ctx context.Context, kvCli kv.Client, dynamicAllocate bool) error
	Pick(ctx context.Context, num int) ([]*metadata.Block, error)
	Stop()
}

func NewAllocator(defaultBlockCapacity int64, selector VolumeSelector) Allocator {
	if defaultBlockCapacity <= 0 {
		defaultBlockCapacity = defaultBlockSize
	} else if defaultBlockCapacity < minimumBlockSize {
		defaultBlockCapacity = minimumBlockSize
	}
	return &allocator{
		blockCapacity:  defaultBlockCapacity,
		selector:       selector,
		allocateTicker: time.NewTicker(time.Second),
	}
}

type allocator struct {
	selector VolumeSelector
	// key: volumeID, value: SkipList of *metadata.Block
	volumeBlockBuffer sync.Map
	kvClient          kv.Client
	mutex             sync.Mutex
	cancel            func()
	cancelCtx         context.Context
	allocateTicker    *time.Ticker
	blockCapacity     int64
}

func (al *allocator) Run(ctx context.Context, kvCli kv.Client, startDynamicAllocate bool) error {
	al.kvClient = kvCli
	pairs, err := al.kvClient.List(ctx, metadata.BlockKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for idx := range pairs {
		pair := pairs[idx]
		bl := &metadata.Block{}
		_err := json.Unmarshal(pair.Value, bl)
		if _err != nil {
			return _err
		}
		v, exist := al.volumeBlockBuffer.Load(bl.VolumeID.Key())
		if !exist {
			v = skiplist.New(skiplist.String)
			al.volumeBlockBuffer.Store(bl.VolumeID.Key(), v)
		}
		l, _ := v.(*skiplist.SkipList)
		if bl.SegmentID == vanus.EmptyID() {
			l.Set(bl.ID.Key(), bl)
		}
	}
	if startDynamicAllocate {
		al.cancelCtx, al.cancel = context.WithCancel(context.Background())
		go al.dynamicAllocateBlockTask(al.cancelCtx)
	}
	return nil
}

func (al *allocator) Pick(ctx context.Context, num int) ([]*metadata.Block, error) {
	al.mutex.Lock()
	defer al.mutex.Unlock()
	blockArr := make([]*metadata.Block, num)

	instances := al.selector.Select(num, al.blockCapacity)
	if len(instances) == 0 {
		return nil, ErrVolumeNotFound
	}
	for idx := 0; idx < num; idx++ {
		ins := instances[idx]
		var skipList *skiplist.SkipList
		v, exist := al.volumeBlockBuffer.Load(ins.GetMeta().ID.Key())
		var err error
		var block *metadata.Block
		if exist {
			skipList, _ = v.(*skiplist.SkipList)
		}

		if !exist || skipList.Len() == 0 {
			block, err = ins.CreateBlock(ctx, al.blockCapacity)
			if err != nil {
				return nil, err
			}
			if err = al.updateBlockInKV(ctx, block); err != nil {
				log.Error(ctx, "save block metadata to kv failed after creating", map[string]interface{}{
					log.KeyError: err,
					"block":      block,
				})
				return nil, err
			}
		} else {
			val := skipList.RemoveFront()
			block, _ = val.Value.(*metadata.Block)
		}
		blockArr[idx] = block
	}
	return blockArr, nil
}

func (al *allocator) Stop() {
	al.cancel()
}

func (al *allocator) dynamicAllocateBlockTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Info(ctx, "the dynamic-allocate task exit", nil)
			return
		case <-al.allocateTicker.C:
			instances := al.selector.GetAllVolume()
			for _, instance := range instances {
				var skipList *skiplist.SkipList
				v, exist := al.volumeBlockBuffer.Load(instance.GetMeta().ID.Key())
				if !exist {
					v = skiplist.New(skiplist.String)
					al.volumeBlockBuffer.Store(instance.GetMeta().ID.Key(), v)
				}
				skipList, _ = v.(*skiplist.SkipList)
				for skipList.Len() < defaultBlockBufferSizePerVolume {
					block, err := instance.CreateBlock(ctx, al.blockCapacity)
					if err != nil {
						log.Warning(ctx, "create block failed", map[string]interface{}{
							"volume_id":   instance.GetMeta().ID,
							"buffer_size": skipList.Len(),
						})
						break
					}
					if err = al.updateBlockInKV(ctx, block); err != nil {
						log.Warning(ctx, "insert block medata to etcd failed", map[string]interface{}{
							"volume_id":   instance.GetMeta().ID,
							"block_id":    block.ID,
							log.KeyError:  err,
							"buffer_size": skipList.Len(),
						})
						break
					}
					log.Info(ctx, "a new block created", map[string]interface{}{
						"volume_id": instance.GetMeta().ID,
						"block_id":  block.ID,
					})
					skipList.Set(block.ID.Key(), block)
				}
			}
		}
	}
}

func (al *allocator) updateBlockInKV(ctx context.Context, block *metadata.Block) error {
	if block == nil {
		return nil
	}
	data, err := json.Marshal(block)
	if err != nil {
		return err
	}
	key := strings.Join([]string{metadata.BlockKeyPrefixInKVStore, block.VolumeID.Key(), block.ID.Key()}, "/")
	return al.kvClient.Set(ctx, key, data)
}
