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
	// this project.
	"github.com/vanus-labs/vanus/internal/controller/eventbus"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/controller/root"
	"github.com/vanus-labs/vanus/internal/controller/tenant"
	"github.com/vanus-labs/vanus/internal/controller/trigger"
	"github.com/vanus-labs/vanus/internal/primitive"
)

type Config struct {
	NodeID               uint16         `yaml:"node_id"`
	Name                 string         `yaml:"name"`
	IP                   string         `yaml:"ip"`
	Port                 int            `yaml:"port"`
	GRPCReflectionEnable bool           `yaml:"grpc_reflection_enable"`
	MetadataConfig       MetadataConfig `yaml:"metadata"`
	Replicas             uint           `yaml:"replicas"`
	SecretEncryptionSalt string         `yaml:"secret_encryption_salt"`
	SegmentCapacity      int64          `yaml:"segment_capacity"`
	ClusterConfig        member.Config  `yaml:"cluster"`
	RootControllerAddr   []string       `yaml:"root_controllers"`
	NoCreateDefaultNs    bool           `yaml:"no_create_default_namespace"`
}

func (c *Config) GetClusterConfig() member.Config {
	c.ClusterConfig.NodeName = c.Name
	return c.ClusterConfig
}

func (c *Config) GetEventbusCtrlConfig() eventbus.Config {
	return eventbus.Config{
		IP:               c.IP,
		Port:             c.Port,
		KVStoreEndpoints: c.ClusterConfig.EtcdEndpoints,
		KVKeyPrefix:      c.MetadataConfig.KeyPrefix,
		Replicas:         c.Replicas,
		Topology:         c.ClusterConfig.Topology,
		SegmentCapacity:  c.SegmentCapacity,
	}
}

func (c *Config) GetSnowflakeConfig() root.Config {
	return root.Config{
		KVEndpoints: c.ClusterConfig.EtcdEndpoints,
		KVPrefix:    c.MetadataConfig.KeyPrefix,
	}
}

func (c *Config) GetControllerAddrs() []string {
	addrs := make([]string, 0)
	for _, v := range c.ClusterConfig.Topology {
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
			ServerList: c.ClusterConfig.EtcdEndpoints,
		},
		SecretEncryptionSalt: c.SecretEncryptionSalt,
		ControllerAddr:       c.GetControllerAddrs(),
	}
}

func (c *Config) GetTenantConfig() tenant.Config {
	return tenant.Config{
		Storage: primitive.KvStorageConfig{
			KeyPrefix:  c.MetadataConfig.KeyPrefix,
			ServerList: c.ClusterConfig.EtcdEndpoints,
		},
		ControllerAddr:    c.GetControllerAddrs(),
		NoCreateDefaultNs: c.NoCreateDefaultNs,
	}
}
