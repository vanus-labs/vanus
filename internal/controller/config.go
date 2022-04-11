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
	"fmt"
	embedetcd "github.com/linkall-labs/embed-etcd"
	"github.com/linkall-labs/vanus/internal/controller/eventbus"
	"github.com/linkall-labs/vanus/internal/controller/trigger"
)

type Config struct {
	Name          string
	Clusters      map[string]string `yaml:"clusters"`
	NetworkConfig NetworkConfig     `yaml:"network"`
	TriggerConfig trigger.Config    `yaml:"trigger"`
	StorageConfig StorageConfig     `yaml:"storage"`
	KVConfig      KVConfig          `yaml:"kv"`
}

func (c *Config) GetEtcdConfig() embedetcd.Config {
	return embedetcd.Config{
		Name:       c.Name,
		DataDir:    c.StorageConfig.DataDir,
		ClientAddr: fmt.Sprintf("%s:%d", c.NetworkConfig.ListenClientIP, c.NetworkConfig.ListenClientPort),
		PeerAddr:   fmt.Sprintf("%s:%d", c.NetworkConfig.PeerIP, c.NetworkConfig.PeerPort),
		Clusters:   c.GetEmbedClusters(),
	}
}

func (c *Config) GetEventbusCtrlConfig() eventbus.Config {
	return eventbus.Config{
		IP:               c.NetworkConfig.ListenClientIP,
		Port:             c.NetworkConfig.ListenClientPort,
		KVStoreEndpoints: c.KVConfig.Endpoints,
		KVKeyPrefix:      c.KVConfig.MetaDataKeyPrefix,
	}
}

func (c *Config) GetEmbedClusters() []string {
	cluster := make([]string, 0)
	for k, v := range c.Clusters {
		cluster = append(cluster, fmt.Sprintf("%s=%s", k, v))
	}
	return cluster
}

type KVConfig struct {
	Endpoints         []string `yaml:"endpoints"`
	MetaDataKeyPrefix string   `yaml:"key_prefix"`
}

type StorageConfig struct {
	DataDir string `yaml:"data_dir"`
}

type NetworkConfig struct {
	ListenClientIP   string `yaml:"listen_client_ip"`
	ListenClientPort int    `json:"listen_client_port"`
	PeerIP           string `yaml:"peer_ip"`
	PeerPort         int    `yaml:"peer_port"`
}
