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

package main

import (
	"flag"
	"os"

	"github.com/vanus-labs/vanus/client/pkg/exporter"
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"

	"github.com/vanus-labs/vanus/internal/gateway"
)

var configPath = flag.String("config", "./config/gateway.yaml", "gateway config file path")

func main() {
	flag.Parse()

	cfg, err := gateway.InitConfig(*configPath)
	if err != nil {
		log.Error().Err(err).Msg("init config error")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()
	cfg.Observability.T.ServerName = "Vanus Gateway"
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetGatewayMetrics, exporter.GetExporter)

	ga := gateway.NewGateway(*cfg)
	if err = ga.Start(ctx); err != nil {
		log.Error().Err(err).Msg("start gateway failed")
		os.Exit(-1)
	}
	log.Info(ctx).Msg("Gateway has started")

	select {
	case <-ctx.Done():
		log.Info(ctx).Msg("received system signal, preparing exit")
	}
	ga.Stop()
	log.Info(ctx).Msg("the gateway has been shutdown gracefully")
}
