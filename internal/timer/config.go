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
	"github.com/linkall-labs/vanus/internal/kv"
	"github.com/linkall-labs/vanus/internal/primitive"
	"github.com/linkall-labs/vanus/internal/timer/leaderelection"
	"github.com/linkall-labs/vanus/internal/timer/timingwheel"

	eventctl "github.com/linkall-labs/vanus/internal/timer/event"
	"github.com/linkall-labs/vanus/internal/timer/eventbus"
)

type Config struct {
	Name                string            `yaml:"name"`
	IP                  string            `yaml:"ip"`
	Port                int               `yaml:"port"`
	EtcdEndpoints       []string          `yaml:"etcd"`
	MetadataConfig      MetadataConfig    `yaml:"metadata"`
	TimingWheelConfig   TimingWheelConfig `yaml:"timingwheel"`
	Replicas            uint              `yaml:"replicas"`
	ControllerEndpoints []string          `yaml:"controllers"`
	EtcdClient          kv.Client
	TimingWheelManager  *timingwheel.TimingWheel
}

const (
	ResourceLockName = "timer"
)

func (c *Config) GetLeaderElectionConfig() *leaderelection.Config {
	return &leaderelection.Config{
		Identity:         "",
		ResourceLockName: ResourceLockName,
		// KeyPrefix:        c.MetadataConfig.KeyPrefix,
		EtcdClient: c.EtcdClient,
	}
}

func (c *Config) GetTimingWheelConfig() *timingwheel.Config {
	return &timingwheel.Config{
		Tick:       c.TimingWheelConfig.Tick,
		WheelSize:  c.TimingWheelConfig.WheelSize,
		Layers:     c.TimingWheelConfig.Layers,
		EtcdClient: c.EtcdClient,
	}
}

func (c *Config) GetEventBusConfig() *eventbus.Config {
	return &eventbus.Config{
		Endpoints: c.ControllerEndpoints,
	}
}

func (c *Config) GetEventConfig() *eventctl.Config {
	return &eventctl.Config{
		Endpoints: c.ControllerEndpoints,
	}
}

type MetadataConfig struct {
	KeyPrefix string `yaml:"key_prefix"`
}

type TimingWheelConfig struct {
	Tick      int64 `yaml:"tick"`
	WheelSize int64 `yaml:"wheel_size"`
	Layers    int64 `yaml:"layers"`
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
