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
	"path/filepath"

	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
	"github.com/linkall-labs/vanus/internal/primitive"
)

type Config struct {
	Name                 string            `yaml:"name"`
	IP                   string            `yaml:"ip"`
	Port                 int               `yaml:"port"`
	GRPCReflectionEnable bool              `yaml:"grpc_reflection_enable"`
	EtcdEndpoints        []string          `yaml:"etcd"`
	DataDir              string            `yaml:"data_dir"`
	MetadataConfig       MetadataConfig    `yaml:"metadata"`
	EtcdConfig           embedetcd.Config  `yaml:"embed_etcd"`
	Topology             map[string]string `yaml:"topology"`
	Replicas             uint              `yaml:"replicas"`
	SecretEncryptionSalt string            `yaml:"secret_encryption_salt"`
	SegmentCapacity      int64             `yaml:"segment_capacity"`
}

func (c *Config) GetEtcdConfig() embedetcd.Config {
	c.EtcdConfig.DataDir = filepath.Join(c.DataDir, c.EtcdConfig.DataDir)
	c.EtcdConfig.Name = c.Name
	return c.EtcdConfig
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
