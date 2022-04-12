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

package eventbus

import (
	"context"
	"encoding/json"
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/internal/kv"
	"strings"
)

type volumeMgr struct {
	ctrl          *controller
	volumeInfoMap map[string]*info.VolumeInfo
	exitCh        chan struct{}
	kvStore       kv.Client
}

func (mgr *volumeMgr) registerVolume(ctx context.Context, volume string) (*info.VolumeInfo, error) {
	return nil, nil
}

func (mgr *volumeMgr) refreshRoutingInfo(volumeId string, serverInfo *info.SegmentServerInfo) {

}

func (mgr *volumeMgr) init(ctx context.Context, ctrl *controller) error {
	mgr.ctrl = ctrl
	mgr.volumeInfoMap = make(map[string]*info.VolumeInfo, 0)
	mgr.kvStore = ctrl.kvStore

	// temporary data for testing, delete later
	mgr.volumeInfoMap["volume-1"] = &info.VolumeInfo{
		Capacity:                 1024 * 1024 * 1024,
		Used:                     0,
		BlockNumbers:             0,
		Blocks:                   map[string]string{},
		PersistenceVolumeClaimID: "volume-1",
	}
	pairs, err := mgr.kvStore.List(ctx, volumeKeyPrefixInKVStore)
	if err != nil {
		return err
	}
	for _, v := range pairs {
		vI := &info.VolumeInfo{}

		if err := json.Unmarshal(v.Value, vI); err != nil {
			return err
		}
		mgr.volumeInfoMap[vI.PersistenceVolumeClaimID] = vI
	}
	return nil
}

func (mgr *volumeMgr) destroy(ctx context.Context) {

}

func (mgr *volumeMgr) get(id string) *info.VolumeInfo {
	return mgr.volumeInfoMap[id]
}

func (mgr *volumeMgr) lookupVolumeByServerID(id string) *info.VolumeInfo {
	return nil
}

func (mgr *volumeMgr) updateVolumeInKV(ctx context.Context, volume *info.VolumeInfo) error {
	data, _ := json.Marshal(volume)
	return mgr.kvStore.Set(ctx, mgr.getVolumeKeyInKVStore(volume.ID()), data)
}

func (mgr *volumeMgr) getVolumeKeyInKVStore(volumeID string) string {
	return strings.Join([]string{volumeKeyPrefixInKVStore, volumeID}, "/")
}
