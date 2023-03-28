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
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"
	triggerpb "github.com/vanus-labs/vanus/proto/pkg/trigger"

	// this project.
	"github.com/vanus-labs/vanus/internal/primitive"
	"github.com/vanus-labs/vanus/internal/trigger"
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
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Msg("failed to listen")
		os.Exit(-1)
	}

	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetTriggerMetrics)
	}

	srv := trigger.NewTriggerServer(cfg.Config)

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
