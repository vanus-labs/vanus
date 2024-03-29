// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package segment

import (
	// standard libraries.
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/metrics"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/signal"
)

const (
	debugModeENV = "SEGMENT_SERVER_DEBUG_MODE"
)

func loadConfig(filename string) (*Config, error) {
	c := new(Config)
	if err := primitive.LoadConfig(filename, c); err != nil {
		return nil, err
	}
	if c.IP == "" {
		c.IP = primitive.GetLocalIP()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func Main(configPath string) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Error().Err(err).Msg("Initialize store config failed.")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()

	debugModel := strings.ToLower(os.Getenv(debugModeENV)) == "true"
	MainExt(ctx, *cfg, debugModel)
}

func MainExt(ctx context.Context, cfg Config, debug bool) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Err(err).Int("port", cfg.Port).Msg("Listen tcp port failed.")
		os.Exit(-1)
	}

	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		cfg.Observability.T.ServerName = "Vanus Store"
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetSegmentServerMetrics)
	}

	srv, err := NewServer(cfg, debug)
	if err != nil {
		log.Error().Err(err).Msg("Create segment server failed.")
		os.Exit(-1)
	}

	if err = srv.Initialize(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("The SegmentServer has initialized failed.")
		os.Exit(-2)
	}

	log.Info(ctx).Str("listen_ip", cfg.IP).Int("listen_port", cfg.Port).Msg("The SegmentServer ready to work.")

	go func() {
		if err = srv.Serve(listener); err != nil {
			log.Error(ctx).Err(err).Msg("The SegmentServer occurred an error.")
			return
		}
	}()

	if err = srv.RegisterToController(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("failed to register self to controller")
		os.Exit(1)
	}

	<-ctx.Done()
	log.Info(ctx).Msg("received system signal, preparing exit")

	log.Info(ctx).Msg("The SegmentServer has been shutdown.")
}
