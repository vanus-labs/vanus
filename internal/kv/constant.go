// Copyright 2023 Linkall Inc.
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

package kv

const (
	keyCore     = "/vanus/core/"
	keyResource = keyCore + "resource/"
	keyExecutor = keyCore + "executors/"

	// eventbus.
	ResourceEventbus          = keyResource + "eventbus"
	ResourceEventlog          = keyResource + "eventlog"
	ResourceSegment           = keyResource + "segment"
	ResourceSegmentOfEventlog = keyResource + "segment_eventlog"
	ResourceVolume            = keyResource + "volume/"
	ResourceVolumeMetadata    = ResourceVolume + "metadata"
	ResourceVolumeBlock       = ResourceVolume + "block"
	ResourceVolumeInstance    = ResourceVolume + "instance"

	// subscription.
	ResourceSubscription = keyResource + "subscriptions"
	MetadataSecret       = keyCore + "secrets"
	MetadataOffset       = keyCore + "offsets"
	TriggerWorker        = keyExecutor + "trigger_workers"

	// leader.
	LeaderLock = keyCore + "resource_lock"
	LeaderInfo = keyCore + "leader_info"

	// snowflake ID.
	ClusterNode  = keyCore + "cluster/nodes"
	ClusterStart = keyCore + "cluster/start_at"
)
