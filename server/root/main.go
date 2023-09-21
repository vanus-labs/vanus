// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package root

import (
	// standard libraries.
	"context"
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"sync"

	// third-party libraries.
	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	// first-party libraries.
	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	errinterceptor "github.com/vanus-labs/vanus/api/grpc/interceptor/errors"
	"github.com/vanus-labs/vanus/pkg/observability"
	"github.com/vanus-labs/vanus/pkg/observability/log"
	"github.com/vanus-labs/vanus/pkg/observability/metrics"

	// this project.
	primitive "github.com/vanus-labs/vanus/pkg"
	"github.com/vanus-labs/vanus/pkg/interceptor/memberinterceptor"
	"github.com/vanus-labs/vanus/pkg/signal"
	"github.com/vanus-labs/vanus/pkg/snowflake"
	"github.com/vanus-labs/vanus/server/controller"
	"github.com/vanus-labs/vanus/server/controller/member"
	"github.com/vanus-labs/vanus/server/controller/root"
)

type Config = controller.Config

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
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Err(err).Msg("failed to listen")
		os.Exit(-1)
	}

	if cfg.Observability.M.Enable || cfg.Observability.T.Enable {
		_ = observability.Initialize(ctx, cfg.Observability, metrics.GetControllerMetrics)
	}

	mem := member.New(cfg.GetClusterConfig())
	if err = mem.Init(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("failed to init member")
		os.Exit(-1)
	}

	snowflakeCtrl := root.NewSnowflakeController(cfg.GetSnowflakeConfig(), mem)
	recoveryOpt := recovery.WithRecoveryHandlerContext(
		func(ctx context.Context, p interface{}) error {
			log.Error(ctx).
				Str(log.KeyError, fmt.Sprintf("%v", p)).
				Bytes("stack", debug.Stack()).
				Msg("goroutine panicked")
			return status.Errorf(codes.Internal, "%v", p)
		},
	)

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			errinterceptor.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recoveryOpt),
			memberinterceptor.UnaryServerInterceptor(mem),
			otelgrpc.UnaryServerInterceptor(),
		),
	)

	if err = snowflakeCtrl.Start(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("start Snowflake Controller failed")
		os.Exit(-1)
	}

	// for debug in developing stage
	if cfg.GRPCReflectionEnable {
		reflection.Register(grpcServer)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctrlpb.RegisterSnowflakeControllerServer(grpcServer, snowflakeCtrl)
	ctrlpb.RegisterPingServerServer(grpcServer, snowflakeCtrl)
	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx).Err(err).Msg("grpc server occurred an error")
		}
		wg.Done()
	}()

	if err = mem.Start(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("failed to start member")
		os.Exit(-2)
	}

	log.Info(ctx).Msg("the grpc server ready to work")

	exit := func() {
		snowflake.Destroy()
		snowflakeCtrl.Stop()
		mem.Stop(ctx)
		grpcServer.GracefulStop()
	}

	<-ctx.Done()
	log.Info(ctx).Msg("received system signal, preparing exit")

	exit()
	wg.Wait()
	log.Info(ctx).Msg("the root controller has been shutdown gracefully")
}
