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

package metadata

import (
	"path"

	"github.com/linkall-labs/vanus/internal/primitive/vanus"
)

const (
	VolumeKeyPrefixInKVStore         = "/vanus/internal/resource/volume/metadata"
	BlockKeyPrefixInKVStore          = "/vanus/internal/resource/volume/block"
	VolumeInstanceKeyPrefixInKVStore = "/vanus/internal/resource/volume/instance"

	EventbusKeyPrefixInKVStore = "/vanus/internal/resource/eventbus"
	EventlogKeyPrefixInKVStore = "/vanus/internal/resource/eventlog"
	SegmentKeyPrefixInKVStore  = "/vanus/internal/resource/segment"

	EventlogSegmentsKeyPrefixInKVStore = "/vanus/internal/resource/segs_of_eventlog"
)

func GetEventbusMetadataKey(ebName string) string {
	return path.Join(EventbusKeyPrefixInKVStore, ebName)
}

func GetEventlogMetadataKey(elID vanus.ID) string {
	return path.Join(EventlogKeyPrefixInKVStore, elID.Key())
}

func GetBlockMetadataKey(volumeID, blockID vanus.ID) string {
	return path.Join(BlockKeyPrefixInKVStore, volumeID.Key(), blockID.Key())
}

func GetSegmentMetadataKey(segmentID vanus.ID) string {
	return path.Join(SegmentKeyPrefixInKVStore, segmentID.Key())
}

func GetEventlogSegmentsMetadataKey(eventlogID, segmentID vanus.ID) string {
	return path.Join(EventlogSegmentsKeyPrefixInKVStore, eventlogID.Key(), segmentID.Key())
}
