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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/vanus-labs/vanus/pkg/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/observability/tracing"
)

func Initialize(ctx context.Context, cfg Config, getCollectors func() []prometheus.Collector) error {
	metrics.Init(ctx, cfg.M, getCollectors)
	tracing.Init(cfg.T)
	return nil
}

type Config struct {
	M metrics.Config `yaml:"metrics"`
	T tracing.Config `yaml:"tracing"`
}
