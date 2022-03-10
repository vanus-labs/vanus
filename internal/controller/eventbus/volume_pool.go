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
	"github.com/linkall-labs/vanus/internal/controller/eventbus/info"
	"github.com/linkall-labs/vanus/observability/log"
	"time"
)

type volumePool struct {
	ctrl          *controller
	volumeInfoMap map[string]*info.VolumeInfo
	exitCh        chan struct{}
}

func (pool *volumePool) init(ctrl *controller) error {
	pool.ctrl = ctrl
	pool.volumeInfoMap = make(map[string]*info.VolumeInfo, 0)

	// temporary data for testing, delete later
	pool.volumeInfoMap["volume-1"] = &info.VolumeInfo{
		Capacity:                 1024 * 1024 * 1024,
		Used:                     0,
		BlockNumbers:             0,
		Blocks:                   map[string]string{},
		PersistenceVolumeClaimID: "volume-1",
	}
	go pool.volumeHealthWatch()
	return nil
}

func (pool *volumePool) destroy() error {
	return nil
}

func (pool *volumePool) get(id string) *info.VolumeInfo {
	return pool.volumeInfoMap[id]
}

func (pool *volumePool) ActivateVolume(vInfo *info.VolumeInfo, sInfo *info.SegmentServerInfo) error {
	// TODO update state in KV store
	vInfo.Activate(sInfo)
	return nil
}

func (pool *volumePool) InactivateVolume(vInfo *info.VolumeInfo) error {
	// TODO update state in KV store
	vInfo.Inactivate()
	return nil
}

func (pool *volumePool) volumeHealthWatch() {
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-pool.exitCh:
			log.Info("volume health watcher exist", nil)
		case <-tick.C:
			for k, v := range pool.volumeInfoMap {
				if v.IsActivity() {
					if !v.IsOnline() {
						v.Inactivate()
						log.Info("the volume has changed to inactive", map[string]interface{}{
							"volume_id": k,
						})
					}
				}
			}
		}
	}
}
