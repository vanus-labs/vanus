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

package observability

import (
	"context"
	"fmt"
	"net/http"

	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/tracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Initialize(cfg Config, metricsFunc func()) error {
	if cfg.M.Enable {
		if metricsFunc != nil {
			metricsFunc()
		}
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.M.GetPort()), nil); err != nil {
				log.Error(context.Background(), "Metrics listen and serve failed.", map[string]interface{}{
					log.KeyError: err,
				})
			}
		}()
		log.Info(context.Background(), "metrics module started", map[string]interface{}{
			"port": cfg.M.Port,
		})
	}

	tracing.Init(cfg.T)
	return nil
}

type Config struct {
	M Metrics        `yaml:"metrics"`
	T tracing.Config `yaml:"tracing"`
}

type Metrics struct {
	Enable bool `yaml:"enable"`
	Port   int  `yaml:"port"`
}

func (m Metrics) GetPort() int {
	if m.Port == 0 {
		return 2112
	}
	return m.Port
}
