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

import embedetcd "github.com/linkall-labs/embed-etcd"

type Config struct {
	IP               string            `yaml:"ip"`
	Port             int               `yaml:"port"`
	KVStoreEndpoints []string          `yaml:"kv_store_endpoints"`
	KVKeyPrefix      string            `yaml:"kv_key_prefix"`
	EtcdConfig       embedetcd.Config  `yaml:"etcd"`
	Replicas         uint              `yaml:"replicas"`
	Topology         map[string]string `yaml:"topology"`
	SegmentCapacity  int64             `yaml:"segment_capacity"`
}
