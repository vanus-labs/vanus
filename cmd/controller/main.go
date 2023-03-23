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
	"fmt"
	"net"
	"os"
	"runtime/debug"
	"sync"

	recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/vanus-labs/vanus/internal/controller/tenant"
	"github.com/vanus-labs/vanus/observability"
	"github.com/vanus-labs/vanus/observability/log"
	"github.com/vanus-labs/vanus/observability/metrics"
	"github.com/vanus-labs/vanus/pkg/util/signal"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"

	"github.com/vanus-labs/vanus/internal/controller"
	"github.com/vanus-labs/vanus/internal/controller/eventbus"
	"github.com/vanus-labs/vanus/internal/controller/member"
	"github.com/vanus-labs/vanus/internal/controller/trigger"
	"github.com/vanus-labs/vanus/internal/primitive/interceptor/errinterceptor"
	"github.com/vanus-labs/vanus/internal/primitive/interceptor/memberinterceptor"
	"github.com/vanus-labs/vanus/internal/primitive/vanus"
)

var configPath = flag.String("config", "./config/controller.yaml",
	"the configuration file of controller")

func main() {
	flag.Parse()

	cfg, err := controller.InitConfig(*configPath)
	if err != nil {
		log.Error().Err(err).Msg("init config error")
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()
	if err = vanus.InitSnowflake(ctx, cfg.RootControllerAddr,
		vanus.NewNode(vanus.ControllerService, cfg.NodeID)); err != nil {
		log.Error(ctx).Err(err).Msg("failed to init id generator")
		os.Exit(-3)
	}

	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error().Err(err).Msg("failed to listen")
		os.Exit(-1)
	}

	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetControllerMetrics)
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
			log.Error(ctx).Err(err).
				Bytes("stack", debug.Stack()).Msg("goroutine panic")
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
		vanus.DestroySnowflake()
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
