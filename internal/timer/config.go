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

package timer

import (
	"time"

	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/timer/leaderelection"
	"github.com/linkall-labs/vanus/internal/timer/timingwheel"
)

type Config struct {
	Name                 string               `yaml:"name"`
	IP                   string               `yaml:"ip"`
	Port                 int                  `yaml:"port"`
	Replicas             uint                 `yaml:"replicas"`
	EtcdEndpoints        []string             `yaml:"etcd"`
	CtrlEndpoints        []string             `yaml:"controllers"`
	MetadataConfig       MetadataConfig       `yaml:"metadata"`
	LeaderElectionConfig LeaderElectionConfig `yaml:"leaderelection"`
	TimingWheelConfig    TimingWheelConfig    `yaml:"timingwheel"`
}

const (
	resourceLockName = "timer"
)

func (c *Config) GetLeaderElectionConfig() *leaderelection.Config {
	return &leaderelection.Config{
		LeaseDuration: c.LeaderElectionConfig.LeaseDuration,
		Name:          resourceLockName,
		KeyPrefix:     c.MetadataConfig.KeyPrefix,
		EtcdEndpoints: c.EtcdEndpoints,
	}
}

func (c *Config) GetTimingWheelConfig() *timingwheel.Config {
	return &timingwheel.Config{
		Tick:          time.Duration(c.TimingWheelConfig.Tick) * time.Second,
		WheelSize:     c.TimingWheelConfig.WheelSize,
		Layers:        c.TimingWheelConfig.Layers,
		KeyPrefix:     c.MetadataConfig.KeyPrefix,
		EtcdEndpoints: c.EtcdEndpoints,
		CtrlEndpoints: c.CtrlEndpoints,
	}
}

type MetadataConfig struct {
	KeyPrefix string `yaml:"key_prefix"`
}

type LeaderElectionConfig struct {
	LeaseDuration int64 `yaml:"lease_duration"`
}

type TimingWheelConfig struct {
	Tick      int64 `yaml:"tick"`
	WheelSize int64 `yaml:"wheel_size"`
	Layers    int64 `yaml:"layers"`
}

func Default(c *Config) {
	if c.LeaderElectionConfig.LeaseDuration == 0 {
		c.LeaderElectionConfig.LeaseDuration = 15
	}
	if c.TimingWheelConfig.Tick == 0 {
		c.TimingWheelConfig.Tick = 1
	}
	if c.TimingWheelConfig.WheelSize == 0 {
		c.TimingWheelConfig.WheelSize = 32
	}
	if c.TimingWheelConfig.Layers == 0 {
		c.TimingWheelConfig.Layers = 4
	}
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	Default(c)
	return c, nil
}
