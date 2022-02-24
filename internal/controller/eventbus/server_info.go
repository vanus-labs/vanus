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
	"crypto/sha256"
	"fmt"
)

type SegmentServerInfo struct {
	id      string
	Address string
	Volume  *VolumeInfo
}

func (info *SegmentServerInfo) ID() string {
	if info.id == "" {
		info.id = fmt.Sprintf("%x", sha256.Sum256([]byte(info.Address)))

	}
	return info.id
}

type VolumeInfo struct {
	Capacity                 int64
	Used                     int64
	BlockNumbers             int
	Blocks                   map[int64]string
	PersistenceVolumeClaimID string
}

func (info *VolumeInfo) ID() string {
	return info.PersistenceVolumeClaimID
}
