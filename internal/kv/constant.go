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
	ResourceEventbus          = "/vanus/core/eventbus_controller/eventbus"
	ResourceEventlog          = "/vanus/core/eventbus_controller/eventlog"
	ResourceSegment           = "/vanus/core/eventbus_controller/segment"
	ResourceSegmentOfEventlog = "/vanus/core/eventbus_controller/segs_of_eventlog"
	ResourceVolumeMetadata    = "/vanus/core/eventbus_controller/volume/metadata"
	ResourceVolumeBlock       = "/vanus/core/eventbus_controller/volume/block"
	ResourceVolumeInstance    = "/vanus/core/eventbus_controller/volume/instance"
	ResourceSubscription      = "/vanus/core/trigger_controller/subscriptions"
	MetadataSecret            = "/vanus/core/trigger_controller/secrets" //nolint:gosec // ok
	MetadataOffset            = "/vanus/core/trigger_controller/offsets"
	TriggerWorker             = "/vanus/core/trigger_controller/trigger_workers"
	LeaderLock                = "/vanus/core/cluster/resource_lock"
	LeaderInfo                = "/vanus/core/cluster/leader_info"
	ClusterNode               = "/vanus/core/cluster/nodes"
	ClusterStart              = "/vanus/core/cluster/start_at"
)
