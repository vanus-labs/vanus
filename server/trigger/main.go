// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package trigger

import (
	// standard libraries.
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	// third-party libraries.
	"google.golang.org/grpc"

	// first-party libraries.
	triggerpb "github.com/vanus-labs/vanus/api/trigger"
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
	if c.IP == "" {
		c.IP = primitive.GetLocalIP()
	}
	c.TriggerAddr = fmt.Sprintf("%s:%d", c.IP, c.Port)
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
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Msg("failed to listen")
		os.Exit(-1)
	}

	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetTriggerMetrics)
	}

	srv := NewTriggerServer(cfg)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	triggerpb.RegisterTriggerWorkerServer(grpcServer, srv)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Info(ctx).Msg("the grpc server ready to work")
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx).Err(err).Msg("grpc server occurred an error")
		}
	}()

	init, _ := srv.(primitive.Initializer)
	if err = init.Initialize(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("the trigger worker has initialized failed")
		os.Exit(1)
	}
	<-ctx.Done()

	closer, _ := srv.(primitive.Closer)
	_ = closer.Close(ctx)
	grpcServer.GracefulStop()

	wg.Wait()
	log.Info(ctx).Msg("trigger worker stopped")
}
