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

	vanus "github.com/vanus-labs/vanus/api/vsr"
	"github.com/vanus-labs/vanus/pkg/kv"
)

const (
	VolumeKeyPrefixInKVStore         = kv.ResourceVolumeMetadata
	BlockKeyPrefixInKVStore          = kv.ResourceVolumeBlock
	VolumeInstanceKeyPrefixInKVStore = kv.ResourceVolumeInstance

	EventlogKeyPrefixInKVStore = kv.ResourceEventlog
	SegmentKeyPrefixInKVStore  = kv.ResourceSegment

	EventlogSegmentsKeyPrefixInKVStore = kv.ResourceSegmentOfEventlog
)

func GetEventbusMetadataKey(id vanus.ID) string {
	return path.Join(kv.ResourceEventbus, id.Key())
}

func GetEventlogMetadataKey(elID vanus.ID) string {
	return path.Join(EventlogKeyPrefixInKVStore, elID.Key())
}

func GetBlockMetadataKey(volumeID, blockID vanus.ID) string {
	return path.Join(kv.ResourceVolumeBlock, volumeID.Key(), blockID.Key())
}

func GetSegmentMetadataKey(segmentID vanus.ID) string {
	return path.Join(SegmentKeyPrefixInKVStore, segmentID.Key())
}

func GetEventlogSegmentsMetadataKey(eventlogID, segmentID vanus.ID) string {
	return path.Join(EventlogSegmentsKeyPrefixInKVStore, eventlogID.Key(), segmentID.Key())
}
