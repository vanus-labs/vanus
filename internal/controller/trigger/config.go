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

package trigger

import (
	"github.com/linkall-labs/vanus/internal/primitive"
)

type Config struct {
	//grpc server config
	Port int `yaml:"port"`
	//start trigger control wait time for trigger heartbeat ,then trigger controller can collect running trigger worker
	//unit second
	WaitTriggerTime int `yaml:"waitTriggerTime"`
	// etcd storage config
	Storage primitive.KvStorageConfig `yaml:"storage"`
}

func Init(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
