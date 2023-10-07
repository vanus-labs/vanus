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

package segment

import (
	// this project.
	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/server/store/config"
)

type Config struct {
	Observability       observability.Config `yaml:"observability"`
	ControllerAddresses []string             `yaml:"controllers"`
	IP                  string               `yaml:"ip"`
	Host                string               `yaml:"host"`
	Port                int                  `yaml:"port"`
	Volume              VolumeInfo           `yaml:"volume"`
	MetaStore           config.SyncStore     `yaml:"meta_store"`
	OffsetStore         config.AsyncStore    `yaml:"offset_store"`
	Raft                config.Raft          `yaml:"raft"`
	VSB                 config.VSB           `yaml:"vsb"`
}

func (c *Config) Validate() error {
	if err := c.MetaStore.Validate(); err != nil {
		return err
	}
	if err := c.OffsetStore.Validate(); err != nil {
		return err
	}
	if err := c.Raft.Validate(); err != nil {
		return err
	}
	return c.VSB.Validate()
}

type VolumeInfo struct {
	ID       uint16 `json:"id"`
	Dir      string `json:"dir"`
	Capacity uint64 `json:"capacity"`
}
