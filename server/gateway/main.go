// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package gateway

import (
	// standard libraries.
	"context"
	"os"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/signal"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
)

func loadConfig(filename string) (*Config, error) {
	c := new(Config)
	err := primitive.LoadConfig(filename, c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func Main(configPath string) {
	cfg, err := loadConfig(configPath)
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

	ga := NewGateway(cfg)

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
