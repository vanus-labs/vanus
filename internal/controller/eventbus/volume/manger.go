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
	"strings"
)

const (
	volumeKeyPrefixInKVStore = "/vanus/internal/resource/volume"
)

type Manager interface {
	Init(ctx context.Context, kvClient kv.Client) error
	GetAllVolume() []server.Instance
	RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error)
	RefreshRoutingInfo(ins server.Instance, srv server.Server)
	GetVolumeInstanceByID(id vanus.ID) server.Instance
	LookupVolumeByServerID(id vanus.ID) server.Instance
	Destroy() error
}

var (
	mgr = &volumeMgr{}
)

func NewVolumeManager() Manager {
	return mgr
}

type volumeMgr struct {
	volInstanceMap map[vanus.ID]server.Instance
	kvCli          kv.Client
}

func (mgr *volumeMgr) RegisterVolume(ctx context.Context, md *metadata.VolumeMetadata) (server.Instance, error) {
	return nil, nil
}

func (mgr *volumeMgr) RefreshRoutingInfo(ins server.Instance, srv server.Server) {

}

func (mgr *volumeMgr) Init(ctx context.Context, kvClient kv.Client) error {
	mgr.volInstanceMap = make(map[vanus.ID]server.Instance, 0)
	mgr.kvCli = kvClient

	pairs, err := mgr.kvCli.List(ctx, volumeKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		md := &metadata.VolumeMetadata{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		mgr.volInstanceMap[md.ID] = server.NewInstance(md)
	}
	return nil
}

func (mgr *volumeMgr) GetVolumeInstanceByID(id vanus.ID) server.Instance {
	//return mgr.volInstanceMap[id]
	return nil
}

func (mgr *volumeMgr) LookupVolumeByServerID(id vanus.ID) server.Instance {
	return nil
}

func (mgr *volumeMgr) GetAllVolume() []server.Instance {
	return nil
}

func (mgr *volumeMgr) Destroy() error {
	return nil
}

func (mgr *volumeMgr) updateVolumeInKV(ctx context.Context, md *metadata.VolumeMetadata) error {
	data, _ := json.Marshal(md)
	return mgr.kvCli.Set(ctx, mgr.getVolumeKeyInKVStore(md.ID), data)
}

func (mgr *volumeMgr) getVolumeKeyInKVStore(id vanus.ID) string {
	return strings.Join([]string{volumeKeyPrefixInKVStore, id.String()}, "/")
}
