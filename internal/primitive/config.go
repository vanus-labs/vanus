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

package primitive

import (
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v3"
)

type KvStorageConfig struct {
	KeyPrefix  string   `yaml:"key_prefix" json:"keyPrefix"`
	ServerList []string `yaml:"server_list" json:"serverList"`
}

func LoadConfig(filename string, config interface{}) error {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	str := os.ExpandEnv(string(b))
	err = yaml.Unmarshal([]byte(str), config)
	if err != nil {
		return err
	}
	return nil
}
