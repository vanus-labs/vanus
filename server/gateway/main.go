// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	// standard libraries.
	"context"
	"os"

	// first-party libraries.
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"

	// this project.
	"github.com/vanus-labs/vanus/internal/gateway"
)

func Main(configPath string) {
	cfg, err := InitConfig(configPath)
	if err != nil {
		log.Error().Err(err).Msg("init config error")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()

	MainExt(ctx, *cfg)
}

func MainExt(ctx context.Context, cfg Config) {
	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		cfg.Observability.T.ServerName = "Vanus Gateway"
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetGatewayMetrics)
	}

	ga := gateway.NewGateway(cfg.Config)

	if err := ga.Start(ctx); err != nil {
		log.Error().Err(err).Msg("start gateway failed")
		os.Exit(-1)
	}

	log.Info(ctx).Msg("Gateway has started")

	<-ctx.Done()
	log.Info(ctx).Msg("received system signal, preparing exit")

	ga.Stop()
	log.Info(ctx).Msg("the gateway has been shutdown gracefully")
}
