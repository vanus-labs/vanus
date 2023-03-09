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

package controller

import (
	"github.com/vanus-labs/vanus/observability"

	"github.com/vanus-labs/vanus/internal/controller/eventbus"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/controller/snowflake"
	"github.com/vanus-labs/vanus/internal/controller/trigger"
	"github.com/vanus-labs/vanus/internal/primitive"
)

type Config struct {
	NodeID               uint16               `yaml:"node_id"`
	Name                 string               `yaml:"name"`
	IP                   string               `yaml:"ip"`
	Port                 int                  `yaml:"port"`
	GRPCReflectionEnable bool                 `yaml:"grpc_reflection_enable"`
	EtcdEndpoints        []string             `yaml:"etcd"`
	DataDir              string               `yaml:"data_dir"`
	MetadataConfig       MetadataConfig       `yaml:"metadata"`
	LeaderElectionConfig LeaderElectionConfig `yaml:"leader_election"`
	Topology             map[string]string    `yaml:"topology"`
	Replicas             uint                 `yaml:"replicas"`
	SecretEncryptionSalt string               `yaml:"secret_encryption_salt"`
	SegmentCapacity      int64                `yaml:"segment_capacity"`
	Observability        observability.Config `yaml:"observability"`
}

type LeaderElectionConfig struct {
	LeaseDuration int64 `yaml:"lease_duration"`
}

const (
	resourceLockName = "controller"
)

func (c *Config) GetMemberConfig() member.Config {
	return member.Config{
		LeaseDuration: c.LeaderElectionConfig.LeaseDuration,
		Name:          resourceLockName,
		EtcdEndpoints: c.EtcdEndpoints,
	}
}

func (c *Config) GetEventbusCtrlConfig() eventbus.Config {
	return eventbus.Config{
		IP:               c.IP,
		Port:             c.Port,
		KVStoreEndpoints: c.EtcdEndpoints,
		KVKeyPrefix:      c.MetadataConfig.KeyPrefix,
		Replicas:         c.Replicas,
		Topology:         c.Topology,
		SegmentCapacity:  c.SegmentCapacity,
	}
}

func (c *Config) GetSnowflakeConfig() snowflake.Config {
	return snowflake.Config{
		KVEndpoints: c.EtcdEndpoints,
		KVPrefix:    c.MetadataConfig.KeyPrefix,
	}
}

func (c *Config) GetControllerAddrs() []string {
	addrs := make([]string, 0)
	for _, v := range c.Topology {
		addrs = append(addrs, v)
	}
	return addrs
}

type MetadataConfig struct {
	KeyPrefix string `yaml:"key_prefix"`
}

func (c *Config) GetTriggerConfig() trigger.Config {
	return trigger.Config{
		Storage: primitive.KvStorageConfig{
			KeyPrefix:  c.MetadataConfig.KeyPrefix,
			ServerList: c.EtcdEndpoints,
		},
		SecretEncryptionSalt: c.SecretEncryptionSalt,
		ControllerAddr:       c.GetControllerAddrs(),
	}
}

func InitConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}
