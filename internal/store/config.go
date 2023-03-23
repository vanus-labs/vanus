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

package store

import (
	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/pkg/util"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/store/config"
)

type Config struct {
	ControllerAddresses []string             `yaml:"controllers"`
	IP                  string               `yaml:"ip"`
	Port                int                  `yaml:"port"`
	Volume              VolumeInfo           `yaml:"volume"`
	MetaStore           config.SyncStore     `yaml:"meta_store"`
	OffsetStore         config.AsyncStore    `yaml:"offset_store"`
	Raft                config.Raft          `yaml:"raft"`
	VSB                 config.VSB           `yaml:"vsb"`
	Observability       observability.Config `yaml:"observability"`
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

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	if err := primitive.LoadConfig(filename, c); err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = util.GetLocalIP()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}
