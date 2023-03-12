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
	"github.com/vanus-labs/vanus/internal/controller/snowflake"
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
		log.Error(context.Background(), "init config error", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}
	listen, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		log.Error(context.Background(), "failed to listen", map[string]interface{}{
			"error": err,
		})
		os.Exit(-1)
	}

	ctx := signal.SetupSignalContext()
	_ = observability.Initialize(ctx, cfg.Observability, metrics.GetControllerMetrics)
	mem := member.New(cfg.GetClusterConfig())
	if err = mem.Init(ctx); err != nil {
		log.Error(ctx, "failed to init member", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// TODO wait server ready
	snowflakeCtrl := snowflake.NewSnowflakeController(cfg.GetSnowflakeConfig(), mem)
	if err = snowflakeCtrl.Start(ctx); err != nil {
		log.Error(ctx, "start Snowflake Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// namespace controller
	namespaceCtrlStv := tenant.NewController(cfg.GetTenantConfig(), mem)
	if err = namespaceCtrlStv.Start(); err != nil {
		log.Error(ctx, "start namespace controller fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	segmentCtrl := eventbus.NewController(cfg.GetEventbusCtrlConfig(), mem)
	if err = segmentCtrl.Start(ctx); err != nil {
		log.Error(ctx, "start EventbusService Controller failed", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	// trigger controller
	triggerCtrlStv := trigger.NewController(cfg.GetTriggerConfig(), mem)
	if err = triggerCtrlStv.Start(); err != nil {
		log.Error(ctx, "start trigger controller fail", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-1)
	}

	if err = mem.Start(ctx); err != nil {
		log.Error(ctx, "failed to start member", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-2)
	}

	recoveryOpt := recovery.WithRecoveryHandlerContext(
		func(ctx context.Context, p interface{}) error {
			log.Error(ctx, "goroutine panicked", map[string]interface{}{
				log.KeyError: fmt.Sprintf("%v", p),
				"stack":      string(debug.Stack()),
			})
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

	ctrlpb.RegisterSnowflakeControllerServer(grpcServer, snowflakeCtrl)
	ctrlpb.RegisterNamespaceControllerServer(grpcServer, namespaceCtrlStv)
	ctrlpb.RegisterEventbusControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterEventlogControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterSegmentControllerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterPingServerServer(grpcServer, segmentCtrl)
	ctrlpb.RegisterTriggerControllerServer(grpcServer, triggerCtrlStv)
	log.Info(ctx, "the grpc server ready to work", nil)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		err = grpcServer.Serve(listen)
		if err != nil {
			log.Error(ctx, "grpc server occurred an error", map[string]interface{}{
				log.KeyError: err,
			})
		}
		wg.Done()
	}()

	exit := func() {
		vanus.DestroySnowflake()
		snowflakeCtrl.Stop()
		triggerCtrlStv.Stop(ctx)
		segmentCtrl.Stop()
		mem.Stop(ctx)
		grpcServer.GracefulStop()
	}

	if err = vanus.InitSnowflake(ctx, cfg.GetControllerAddrs(),
		vanus.NewNode(vanus.ControllerService, cfg.NodeID)); err != nil {
		log.Error(ctx, "failed to init id generator", map[string]interface{}{
			log.KeyError: err,
		})
		os.Exit(-3)
	}

	select {
	case <-ctx.Done():
		log.Info(ctx, "received system signal, preparing exit", nil)
	case <-segmentCtrl.StopNotify():
		log.Info(ctx, "received segment controller ready to stop, preparing exit", nil)
	}
	exit()
	wg.Wait()
	log.Info(ctx, "the controller has been shutdown gracefully", nil)
}
