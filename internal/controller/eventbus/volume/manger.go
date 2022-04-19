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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/metadata"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/server"
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive/vanus"
	"github.com/linkall-labs/vanus/observability/log"
	"path/filepath"
	"sync"
)

const (
	volumeKeyPrefixInKVStore         = "/vanus/internal/resource/volume/metadata"
	volumeInstanceKeyPrefixInKVStore = "/vanus/internal/resource/volume/instance"
)

type Manager interface {
	Init(ctx context.Context, kvClient kv.Client) error
	GetAllVolume() []server.Instance
	RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error)
	UpdateRouting(ctx context.Context, ins server.Instance, srv server.Server)
	GetVolumeInstanceByID(id vanus.ID) server.Instance
	LookupVolumeByServerID(id vanus.ID) server.Instance
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
	key := filepath.Join(volumeKeyPrefixInKVStore, md.ID.String())
	if err := mgr.kvCli.Set(ctx, key, data); err != nil {
		return nil, err
	}
	mgr.volInstanceMap.Store(md.ID.Key(), ins)
	return ins, nil
}

func (mgr *volumeMgr) Init(ctx context.Context, kvClient kv.Client) error {
	mgr.kvCli = kvClient

	// TODO add volume when register
	pairs, err := mgr.kvCli.List(ctx, volumeKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		md := &metadata.VolumeMetadata{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		ins := server.NewInstance(md)
		mgr.volInstanceMap.Store(md.ID.Key(), ins)
	}
	pairs, err = mgr.kvCli.List(ctx, volumeInstanceKeyPrefixInKVStore)
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

		srv, err := server.NewSegmentServerWithID(obj.ServerID, obj.Address)
		if err != nil {
			log.Warning(ctx, "create segment server failed failed", map[string]interface{}{
				log.KeyError: err,
				"volume_id":  v.Key,
				"address":    obj.Address,
			})
			continue
		}
		id, _ := vanus.NewIDFromString(filepath.Base(v.Key))
		ins, exist := mgr.volInstanceMap.Load(id.Key())
		if exist {
			mgr.UpdateRouting(ctx, ins.(server.Instance), srv)
			if err = mgr.serverMgr.AddServer(ctx, srv); err != nil {
				log.Warning(ctx, "add server to server manager failed", map[string]interface{}{
					log.KeyError: err,
					"volume_id":  v.Key,
					"address":    obj.Address,
				})
			}
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

func (mgr *volumeMgr) GetAllVolume() []server.Instance {
	results := make([]server.Instance, 0)
	mgr.volInstanceMap.Range(func(key, value interface{}) bool {
		results = append(results, value.(server.Instance))
		return true
	})
	return results
}

func (mgr *volumeMgr) UpdateRouting(ctx context.Context, ins server.Instance, srv server.Server) {
	key := filepath.Join(volumeInstanceKeyPrefixInKVStore, ins.ID().String())
	if srv != nil {
		mgr.volInstanceMapByServerID.Store(srv.ID().Key(), ins)
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
	} else {
		mgr.volInstanceMapByServerID.Delete(ins.GetServer().ID())
		if err := mgr.kvCli.Delete(ctx, key); err != nil {
			log.Warning(ctx, "delete runtime info of volume instance to kv failed", map[string]interface{}{
				"volume_id":  ins.ID(),
				log.KeyError: err,
			})
		}
	}
	ins.SetServer(srv)
	mgr.volInstanceMap.Store(ins.ID().Key(), ins)
}
