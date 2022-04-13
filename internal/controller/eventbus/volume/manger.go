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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"strings"
)

const (
	volumeKeyPrefixInKVStore = "/vanus/internal/resource/volume"
)

type Manager interface {
	RegisterVolume(ctx context.Context, md *Metadata) (Instance, error)
	RefreshRoutingInfo(ins Instance, serverInfo *info.SegmentServerInfo)
	Init(ctx context.Context, kvClient kv.Client) error
	GetVolumeByID(id string) Instance
	LookupVolumeByServerID(id string) Instance
	GetAllVolume() []Instance
}

func NewVolumeManager() Manager {
	return &volumeMgr{}
}

type volumeMgr struct {
	volInstanceMap map[string]Instance
	exitCh         chan struct{}
	kvCli          kv.Client
}

func (mgr *volumeMgr) RegisterVolume(ctx context.Context, md *Metadata) (Instance, error) {
	return nil, nil
}

func (mgr *volumeMgr) RefreshRoutingInfo(ins Instance, serverInfo *info.SegmentServerInfo) {

}

func (mgr *volumeMgr) Init(ctx context.Context, kvClient kv.Client) error {
	mgr.volInstanceMap = make(map[string]Instance, 0)
	mgr.kvCli = kvClient

	pairs, err := mgr.kvCli.List(ctx, volumeKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		md := &Metadata{}
		if err := json.Unmarshal(v.Value, md); err != nil {
			return err
		}
		mgr.volInstanceMap[md.ID] = newInstance(md)
	}
	return nil
}

func (mgr *volumeMgr) GetVolumeByID(id string) Instance {
	return mgr.volInstanceMap[id]
}

func (mgr *volumeMgr) LookupVolumeByServerID(id string) Instance {
	return nil
}

func (mgr *volumeMgr) GetAllVolume() []Instance {
	return nil
}

func (mgr *volumeMgr) updateVolumeInKV(ctx context.Context, md *Metadata) error {
	data, _ := json.Marshal(md)
	return mgr.kvCli.Set(ctx, mgr.getVolumeKeyInKVStore(md.ID), data)
}

func (mgr *volumeMgr) getVolumeKeyInKVStore(volumeID string) string {
	return strings.Join([]string{volumeKeyPrefixInKVStore, volumeID}, "/")
}

func (mgr *volumeMgr) getVolumeInfos() []Instance {
	return nil
}
