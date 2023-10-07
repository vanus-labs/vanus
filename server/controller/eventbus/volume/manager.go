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

//go:generate mockgen -source=manager.go -destination=mock_manager.go -package=volume
package volume

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"

	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/pkg/observability/log"

	"github.com/vanus-labs/vanus/pkg/kv"
	"github.com/vanus-labs/vanus/server/controller/eventbus/metadata"
	"github.com/vanus-labs/vanus/server/controller/eventbus/server"
)

type Manager interface {
	Init(ctx context.Context, kvClient kv.Client) error
	GetAllActiveVolumes() []server.Instance
	RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error)
	UpdateRouting(ctx context.Context, ins server.Instance, srv server.Server)
	GetVolumeInstanceByID(id vanus.ID) server.Instance
	LookupVolumeByID(id uint64) server.Instance
	GetBlocksOfVolume(ctx context.Context, instance server.Instance) (map[uint64]*metadata.Block, error)
}

var mgr = &volumeMgr{}

func NewVolumeManager(serverMgr server.Manager) Manager {
	mgr.serverMgr = serverMgr
	return mgr
}

type volumeMgr struct {
	// volumeID(vanus.ID) server.Instance
	volInstanceMap sync.Map
	// volumeID(uint64) server.Instance
	volInstanceByPhysicalVolumeID sync.Map
	kvCli                         kv.Client
	serverMgr                     server.Manager
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
		blockPairs, err := mgr.kvCli.List(ctx,
			filepath.Join(metadata.BlockKeyPrefixInKVStore, md.ID.Key()))
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
			Address  string `json:"address"`
			VolumeID uint64 `json:"volume_id"`
		})
		if err = json.Unmarshal(v.Value, obj); err != nil {
			log.Warn(ctx).Err(err).
				Str("volume_id", v.Key).
				Msg("unmarshal volume instance runtime info failed")
			continue
		}

		volumeID, err := vanus.NewIDFromString(filepath.Base(v.Key))
		if err != nil {
			return err
		}
		ins, exist := mgr.volInstanceMap.Load(volumeID.Key())
		if !exist {
			log.Warn(ctx).
				Str("address", obj.Address).
				Stringer("volume_id", volumeID).
				Msg("the invalid segment server info founded")
			continue
		}

		srv, err := server.NewSegmentServerWithVolumeID(obj.VolumeID, obj.Address)
		if err != nil {
			log.Warn(ctx).Err(err).
				Str("address", obj.Address).
				Stringer("volume_id", volumeID).
				Msg("failed to create segment server")
			continue
		}
		if !srv.IsActive(ctx) {
			srv = nil
		}

		mgr.UpdateRouting(ctx, ins.(server.Instance), srv)
		if err = mgr.serverMgr.AddServer(ctx, srv); err != nil {
			log.Warn(ctx).Err(err).
				Str("address", obj.Address).
				Stringer("volume_id", volumeID).
				Msg("add server to server manager failed")
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

func (mgr *volumeMgr) LookupVolumeByID(id uint64) server.Instance {
	v, exist := mgr.volInstanceByPhysicalVolumeID.Load(id)
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
	key := filepath.Join(metadata.VolumeInstanceKeyPrefixInKVStore, ins.ID().String())
	if srv == nil {
		if err := mgr.kvCli.Delete(ctx, key); err != nil {
			log.Warn(ctx).Err(err).
				Stringer("volume_id", ins.ID()).
				Msg("delete runtime info of volume instance to kv failed")
		}
		return
	}
	if !srv.IsActive(ctx) {
		return
	}
	v := new(struct {
		Address  string `json:"address"`
		VolumeID uint64 `json:"volume_id"`
	})
	v.Address = srv.Address()
	v.VolumeID = srv.VolumeID()
	data, _ := json.Marshal(v)
	if err := mgr.kvCli.Set(ctx, key, data); err != nil {
		log.Warn(ctx).Err(err).
			Stringer("volume_id", ins.ID()).
			Uint64("volume", srv.VolumeID()).
			Str("address", srv.Address()).
			Msg("failed to save runtime info of volume instance to kv")
	}
	mgr.volInstanceByPhysicalVolumeID.Store(srv.VolumeID(), ins)
	ins.SetServer(srv)
	mgr.volInstanceMap.Store(ins.ID().Key(), ins)
}

func (mgr *volumeMgr) GetBlocksOfVolume(ctx context.Context,
	instance server.Instance,
) (map[uint64]*metadata.Block, error) {
	pairs, err := mgr.kvCli.List(ctx, strings.Join([]string{
		metadata.BlockKeyPrefixInKVStore,
		instance.ID().String(),
	}, "/"))
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
