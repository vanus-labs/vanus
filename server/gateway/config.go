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

package gateway

import (
	// third-party libraries.
	"google.golang.org/grpc/credentials/insecure"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability"

	// this project.
	"github.com/vanus-labs/vanus/server/gateway/auth"
	"github.com/vanus-labs/vanus/server/gateway/proxy"
)

const (
	defaultProxyPort = 8080
	defaultSinkPort  = 8082
)

type Config struct {
	Observability        observability.Config `yaml:"observability"`
	Port                 int                  `yaml:"port"`
	SinkPort             int                  `yaml:"sink_port"`
	ControllerAddr       []string             `yaml:"controllers"`
	GRPCReflectionEnable bool                 `yaml:"grpc_reflection_enable"`
	Auth                 Auth                 `yaml:"auth"`
}

type Auth struct {
	Disable bool `yaml:"disable"`
}

func (c Config) GetProxyConfig() proxy.Config {
	cfg := proxy.Config{
		Endpoints:              c.ControllerAddr,
		SinkPort:               c.SinkPort,
		ProxyPort:              c.Port,
		CloudEventReceiverPort: c.GetCloudEventReceiverPort(),
		GRPCReflectionEnable:   c.GRPCReflectionEnable,
		Credentials:            insecure.NewCredentials(),
	}
	if cfg.ProxyPort == 0 {
		cfg.ProxyPort = defaultProxyPort
	}
	if cfg.SinkPort == 0 {
		cfg.SinkPort = defaultSinkPort
	}
	cfg.AuthCfg = auth.Config{
		Disable:          c.Auth.Disable,
		OpenSubscription: false,
		OpenEventbus:     false,
	}
	return cfg
}

func (c Config) GetCloudEventReceiverPort() int {
	if c.Port == 0 {
		return defaultProxyPort + 1
	}
	return c.Port + 1
}
