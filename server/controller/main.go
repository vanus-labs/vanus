// SPDX-FileCopyrightText: 2023 Linkall Inc.
//
// SPDX-License-Identifier: Apache-2.0

package controller

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
	"github.com/vanus-labs/vanus/server/controller/eventbus"
	"github.com/vanus-labs/vanus/server/controller/member"
	"github.com/vanus-labs/vanus/server/controller/tenant"
	"github.com/vanus-labs/vanus/server/controller/trigger"
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
	if err := snowflake.Initialize(ctx, cfg.RootControllerAddr,
		snowflake.NewNode(snowflake.ControllerService, cfg.NodeID)); err != nil {
		log.Error(ctx).Err(err).Msg("failed to init id generator")
		os.Exit(-3)
	}

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

	tenantCtrlStv := tenant.NewController(cfg.GetTenantConfig(), mem)
	segmentCtrl := eventbus.NewController(cfg.GetEventbusCtrlConfig(), mem)
	triggerCtrlStv := trigger.NewController(cfg.GetTriggerConfig(), mem)

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
		grpc.ChainStreamInterceptor(
			errinterceptor.StreamServerInterceptor(),
			recovery.StreamServerInterceptor(recoveryOpt),
			memberinterceptor.StreamServerInterceptor(mem),
			otelgrpc.StreamServerInterceptor(),
		),
		grpc.ChainUnaryInterceptor(
			errinterceptor.UnaryServerInterceptor(),
			recovery.UnaryServerInterceptor(recoveryOpt),
			memberinterceptor.UnaryServerInterceptor(mem),
			otelgrpc.UnaryServerInterceptor(),
		),
	)

	// for debug in developing stage
	if cfg.GRPCReflectionEnable {
		reflection.Register(grpcServer)
	}

	ctrlpb.RegisterPingServerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterNamespaceControllerServer(grpcServer, tenantCtrlStv)
	ctrlpb.RegisterAuthControllerServer(grpcServer, tenantCtrlStv)
	ctrlpb.RegisterEventbusControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterEventlogControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterSegmentControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterTriggerControllerServer(grpcServer, triggerCtrlStv)

	log.Info(ctx).Msg("the grpc server ready to work")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx).Err(err).Msg("grpc server occurred an error")
		}
		wg.Done()
	}()

	if err = tenantCtrlStv.Start(); err != nil {
		log.Error(ctx).Err(err).Msg("start namespace controller fail")
		os.Exit(-1)
	}

	if err = segmentCtrl.Start(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("start EventbusService Controller failed")
		os.Exit(-1)
	}

	if err = triggerCtrlStv.Start(); err != nil {
		log.Error(ctx).Err(err).Msg("start trigger controller fail")
		os.Exit(-1)
	}

	if err = mem.Start(ctx); err != nil {
		log.Error(ctx).Err(err).Msg("failed to start member")
		os.Exit(-2)
	}

	exit := func() {
		snowflake.Destroy()
		triggerCtrlStv.Stop(ctx)
		segmentCtrl.Stop()
		mem.Stop(ctx)
		grpcServer.GracefulStop()
	}

	select {
	case <-ctx.Done():
		log.Info(ctx).Msg("received system signal, preparing exit")
	case <-segmentCtrl.StopNotify():
		log.Info(ctx).Msg("received segment controller ready to stop, preparing exit")
	}
	exit()
	wg.Wait()
	log.Info(ctx).Msg("the controller has been shutdown gracefully")
}
