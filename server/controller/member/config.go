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

package member

type Config struct {
	LeaseDurationInSecond int               `yaml:"lease_duration_in_sec"`
	EtcdEndpoints         []string          `yaml:"etcd"`
	Topology              map[string]string `yaml:"topology"`
	ComponentName         string            `yaml:"component_name"`
	NodeName              string            `yaml:"-"`
}
