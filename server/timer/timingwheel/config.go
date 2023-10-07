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

package timingwheel

import (
	"time"
)

type Config struct {
	Tick          time.Duration `yaml:"tick"`
	WheelSize     int64         `yaml:"wheel_size"`
	Layers        int64         `yaml:"layers"`
	KeyPrefix     string        `yaml:"key_prefix"`
	EtcdEndpoints []string      `yaml:"etcd"`
	CtrlEndpoints []string      `yaml:"controllers"`
}
