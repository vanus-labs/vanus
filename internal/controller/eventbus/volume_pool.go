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

import "github.com/linkall-labs/vanus/internal/controller/eventbus/info"

type volumePool struct {
	ctrl          *controller
	volumeInfoMap map[string]*info.VolumeInfo
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
	return nil
}

func (pool *volumePool) destroy() error {
	return nil
}

func (pool *volumePool) get(id string) *info.VolumeInfo {
	return pool.volumeInfoMap[id]
}

func (pool *volumePool) bindSegmentServer(vInfo *info.VolumeInfo, sInfo *info.SegmentServerInfo) error {
	vInfo.AssignedSegmentServerID = sInfo.ID()
	return nil
}

func (pool *volumePool) release(vInfo *info.VolumeInfo) error {
	return nil
}
