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

package volume

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"

	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
)

type Manager interface {
	Init(ctx context.Context, kvClient kv.Client) error
	GetAllActiveVolumes() []server.Instance
	RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error)
	UpdateRouting(ctx context.Context, ins server.Instance, srv server.Server)
	GetVolumeInstanceByID(id vanus.ID) server.Instance
	LookupVolumeByServerID(id vanus.ID) server.Instance
	GetBlocksOfVolume(ctx context.Context, instance server.Instance) (map[uint64]*metadata.Block, error)
}

var (
	mgr = &volumeMgr{}
)

func NewVolumeManager(serverMgr server.Manager) Manager {
	mgr.serverMgr = serverMgr
	return mgr
}

type volumeMgr struct {
	// volumeID server.Instance
	volInstanceMap sync.Map
	// serverId server.Instance
	volInstanceMapByServerID sync.Map
	kvCli                    kv.Client
	serverMgr                server.Manager
}

func (mgr *volumeMgr) RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error) {
	if v, exist := mgr.volInstanceMap.Load(md.ID.Key()); exist {
		return v.(server.Instance), nil
	}
	ins := server.NewInstance(md)
	data, _ := json.Marshal(md)
	key := filepath.Join(metadata.VolumeKeyPrefixInKVStore, md.ID.String())
	if err := mgr.kvCli.Set(ctx, key, data); err != nil {
		return nil, err
	}
	mgr.volInstanceMap.Store(md.ID.Key(), ins)
	return ins, nil
}

func (mgr *volumeMgr) Init(ctx context.Context, kvClient kv.Client) error {
	mgr.kvCli = kvClient

	pairs, err := mgr.kvCli.List(ctx, metadata.VolumeKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		md := &metadata.VolumeMetadata{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		// load block meta in this volume
		blockPairs, err := mgr.kvCli.List(ctx, filepath.Join(metadata.BlockKeyPrefixInKVStore, md.ID.Key()))
		if err != nil {
			return err
		}
		if md.Blocks == nil {
			md.Blocks = map[uint64]*metadata.Block{}
		}
		for _, vv := range blockPairs {
			bl := &metadata.Block{}
			if err := json.Unmarshal(vv.Value, bl); err != nil {
				return err
			}
			md.Blocks[bl.ID.Uint64()] = bl
		}

		ins := server.NewInstance(md)
		mgr.volInstanceMap.Store(md.ID.Key(), ins)
	}
	pairs, err = mgr.kvCli.List(ctx, metadata.VolumeInstanceKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		obj := new(struct {
			Address  string   `json:"address"`
			ServerID vanus.ID `json:"server_id"`
		})
		if err = json.Unmarshal(v.Value, obj); err != nil {
			log.Warning(ctx, "unmarshal volume instance runtime info failed", map[string]interface{}{
				log.KeyError: err,
				"volume_id":  v.Key,
			})
			continue
		}

		volumeID, _ := vanus.NewIDFromString(filepath.Base(v.Key))
		ins, exist := mgr.volInstanceMap.Load(volumeID.Key())
		if !exist {
			log.Warning(ctx, "the invalid segment server info founded", map[string]interface{}{
				"server_id": obj.ServerID,
				"addr":      obj.Address,
				"volume_id": volumeID,
			})
			continue
		}

		srv, err := server.NewSegmentServerWithID(obj.ServerID, obj.Address)
		if err != nil {
			log.Warning(ctx, "create segment server failed failed", map[string]interface{}{
				log.KeyError: err,
				"volume_id":  v.Key,
				"address":    obj.Address,
			})
			continue
		}
		if !srv.IsActive(ctx) {
			// TODO (wenfeng): remove from kv store
			srv = nil
			continue
		}

		mgr.UpdateRouting(ctx, ins.(server.Instance), srv)
		if err = mgr.serverMgr.AddServer(ctx, srv); err != nil {
			log.Warning(ctx, "add server to server manager failed", map[string]interface{}{
				log.KeyError: err,
				"volume_id":  v.Key,
				"address":    obj.Address,
			})
		}
	}
	return nil
}

func (mgr *volumeMgr) GetVolumeInstanceByID(id vanus.ID) server.Instance {
	v, exist := mgr.volInstanceMap.Load(id.Key())
	if !exist {
		return nil
	}
	return v.(server.Instance)
}

func (mgr *volumeMgr) LookupVolumeByServerID(id vanus.ID) server.Instance {
	v, exist := mgr.volInstanceMapByServerID.Load(id.Key())
	if !exist {
		return nil
	}
	return v.(server.Instance)
}

func (mgr *volumeMgr) GetAllActiveVolumes() []server.Instance {
	results := make([]server.Instance, 0)
	mgr.volInstanceMap.Range(func(key, value interface{}) bool {
		srv, _ := value.(server.Instance)
		if srv.GetServer() != nil && srv.GetServer().IsActive(context.Background()) {
			results = append(results, srv)
		}
		return true
	})
	return results
}

func (mgr *volumeMgr) UpdateRouting(ctx context.Context, ins server.Instance, srv server.Server) {
	// TODO when to persist to kv
	key := filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, ins.ID().String())
	if srv == nil {
		if err := mgr.kvCli.Delete(ctx, key); err != nil {
			log.Warning(ctx, "delete runtime info of volume instance to kv failed", map[string]interface{}{
				"volume_id":  ins.ID(),
				log.KeyError: err,
			})
		}
		return
	}
	if !srv.IsActive(ctx) {
		return
	}
	v := new(struct {
		Address  string   `json:"address"`
		ServerID vanus.ID `json:"server_id"`
	})
	v.Address = srv.Address()
	v.ServerID = srv.ID()
	data, _ := json.Marshal(v)
	if err := mgr.kvCli.Set(ctx, key, data); err != nil {
		log.Warning(ctx, "save runtime info of volume instance to kv failed", map[string]interface{}{
			"volume_id":  ins.ID(),
			"server_id":  srv.ID(),
			"address":    srv.Address(),
			log.KeyError: err,
		})
	}
	mgr.volInstanceMapByServerID.Store(srv.ID().Key(), ins)
	ins.SetServer(srv)
	mgr.volInstanceMap.Store(ins.ID().Key(), ins)
}

func (mgr *volumeMgr) GetBlocksOfVolume(ctx context.Context,
	instance server.Instance) (map[uint64]*metadata.Block, error) {
	pairs, err := mgr.kvCli.List(ctx, strings.Join([]string{metadata.BlockKeyPrefixInKVStore,
		instance.ID().String()}, "/"))
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]*metadata.Block, len(pairs))
	for idx := range pairs {
		pair := pairs[idx]
		bl := &metadata.Block{}
		err := json.Unmarshal(pair.Value, bl)
		if err != nil {
			return nil, err
		}
		result[bl.ID.Uint64()] = bl
	}
	return result, nil
}
