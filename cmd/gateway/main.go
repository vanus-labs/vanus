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
	"context"
	"flag"
	"os"

	"github.com/linkall-labs/vanus/internal/gateway"
	"github.com/linkall-labs/vanus/observability"
	"github.com/linkall-labs/vanus/observability/log"
	"github.com/linkall-labs/vanus/observability/metrics"
	"github.com/linkall-labs/vanus/pkg/util/signal"
)

var (
	configPath = flag.String("config", "./config/gateway.yaml", "gateway config file path")
)

func main() {
	flag.Parse()

	cfg, err := gateway.InitConfig(*configPath)
	if err != nil {
		log.Error(context.Background(), "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()
	ga := gateway.NewGateway(*cfg)

	if err = ga.Start(ctx); err != nil {
		log.Error(context.Background(), "start gateway failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	cfg.Observability.T.ServerName = "Vanus Gateway"
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetGatewayMetrics)
	log.Info(ctx, "Gateway has started", nil)
	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	}
	ga.Stop()
	log.Info(ctx, "the gateway has been shutdown gracefully", nil)
}
